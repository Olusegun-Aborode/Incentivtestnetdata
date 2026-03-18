"""
Neon (serverless Postgres) loader with connection pooling,
bulk insert via COPY, and extraction state management.
"""

import io
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
load_dotenv('.env.neon')


class NeonLoader:
    """Handles all Neon database operations: bulk inserts, upserts, state tracking."""

    def __init__(self, database_url: Optional[str] = None) -> None:
        self.database_url = database_url or os.getenv("NEON_DATABASE_URL")
        if not self.database_url:
            raise RuntimeError("NEON_DATABASE_URL not set. Check .env.neon file.")
        self._conn = None

    @property
    def conn(self):
        """Lazy connection with auto-reconnect on dead connections."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.database_url)
            self._conn.autocommit = False
            return self._conn

        # Check if the connection is actually alive (catches "No route to host"
        # and other TCP-level disconnects that don't set .closed = True)
        try:
            cur = self._conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            print("  [NeonLoader] Connection lost — reconnecting...")
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = psycopg2.connect(self.database_url)
            self._conn.autocommit = False

        return self._conn

    def reconnect(self) -> None:
        """Force a fresh connection (call after unrecoverable errors)."""
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass
        self._conn = psycopg2.connect(self.database_url)
        self._conn.autocommit = False
        print("  [NeonLoader] Reconnected to Neon.")

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Schema setup
    # ------------------------------------------------------------------

    def setup_schema(self, schema_path: str = "migration/neon_schema.sql") -> None:
        """Execute the full schema SQL to set up tables."""
        with open(schema_path, "r") as f:
            schema_sql = f.read()
        cursor = self.conn.cursor()
        try:
            cursor.execute(schema_sql)
            self.conn.commit()
            print("Schema setup complete.")
        except Exception as e:
            self.conn.rollback()
            print(f"Schema setup error: {e}")
            raise
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Bulk insert via COPY (fastest for large datasets)
    # ------------------------------------------------------------------

    def copy_dataframe(self, table: str, df: pd.DataFrame, columns: Optional[List[str]] = None) -> int:
        """
        Bulk insert a DataFrame using PostgreSQL COPY.
        Returns the number of rows inserted.
        Skips rows that violate unique constraints by using a temp table.
        Falls back to execute_values if COPY fails (e.g., due to special chars).
        """
        if df.empty:
            return 0

        cols = columns or list(df.columns)
        df_subset = df[cols].copy()

        # Replace NaN/NaT with None for proper NULL handling
        df_subset = df_subset.where(pd.notnull(df_subset), None)

        # Try COPY first, fall back to execute_values
        try:
            return self._copy_via_temp_table(table, df_subset, cols)
        except Exception as copy_err:
            # COPY failed (likely due to special chars in data fields)
            # Fall back to row-by-row execute_values
            try:
                return self._insert_via_execute_values(table, df_subset, cols)
            except Exception as insert_err:
                raise RuntimeError(
                    f"Both COPY and execute_values failed for {table}. "
                    f"COPY error: {copy_err} | Insert error: {insert_err}"
                )

    def _copy_via_temp_table(self, table: str, df_subset: pd.DataFrame, cols: List[str]) -> int:
        """Fast bulk insert via COPY + temp table."""
        temp_table = f"_tmp_{table}_{int(datetime.utcnow().timestamp())}"
        cursor = self.conn.cursor()

        try:
            col_list = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
            cursor.execute(
                sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                    sql.Identifier(temp_table),
                    sql.Identifier(table),
                )
            )

            # Use tab-separated format to avoid issues with commas in hex data
            buf = io.StringIO()
            df_subset.to_csv(buf, index=False, header=False, sep='\t',
                             na_rep="\\N", quoting=3)  # QUOTE_NONE
            buf.seek(0)

            copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT TEXT, NULL '\\N')").format(
                sql.Identifier(temp_table),
                col_list,
            )
            cursor.copy_expert(copy_sql.as_string(self.conn), buf)

            insert_sql = sql.SQL(
                "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT DO NOTHING"
            ).format(
                sql.Identifier(table),
                col_list,
                col_list,
                sql.Identifier(temp_table),
            )
            cursor.execute(insert_sql)
            rows_inserted = cursor.rowcount
            self.conn.commit()
            return rows_inserted

        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    def _insert_via_execute_values(self, table: str, df_subset: pd.DataFrame, cols: List[str]) -> int:
        """Fallback insert using execute_values (slower but handles special chars)."""
        cursor = self.conn.cursor()
        try:
            col_list = ", ".join(f'"{c}"' for c in cols)
            placeholders = ", ".join(["%s"] * len(cols))
            query = f'INSERT INTO "{table}" ({col_list}) VALUES %s ON CONFLICT DO NOTHING'

            values = []
            for row in df_subset.itertuples(index=False, name=None):
                values.append(tuple(None if pd.isna(v) else v for v in row))

            execute_values(cursor, query, values, page_size=500)
            rows_inserted = cursor.rowcount
            self.conn.commit()
            return rows_inserted
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Upsert via execute_values (medium batches)
    # ------------------------------------------------------------------

    def upsert_dataframe(
        self,
        table: str,
        df: pd.DataFrame,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> int:
        """
        Upsert DataFrame rows. On conflict, either DO NOTHING or update specified columns.
        """
        if df.empty:
            return 0

        cols = list(df.columns)
        col_list = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in cols)
        conflict_list = sql.SQL(", ").join(sql.Identifier(c) for c in conflict_columns)

        if update_columns:
            set_clause = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in update_columns
            )
            on_conflict = sql.SQL("ON CONFLICT ({}) DO UPDATE SET {}").format(
                conflict_list, set_clause
            )
        else:
            on_conflict = sql.SQL("ON CONFLICT ({}) DO NOTHING").format(conflict_list)

        query = sql.SQL("INSERT INTO {} ({}) VALUES %s {}").format(
            sql.Identifier(table), col_list, on_conflict
        )

        # Convert DataFrame to list of tuples, replacing NaN with None
        df_clean = df.where(pd.notnull(df), None)
        values = [tuple(row) for row in df_clean.itertuples(index=False, name=None)]

        cursor = self.conn.cursor()
        try:
            template = sql.SQL("({})").format(
                sql.SQL(", ").join(sql.Placeholder() for _ in cols)
            ).as_string(self.conn)
            execute_values(cursor, query.as_string(self.conn), values, template=template, page_size=1000)
            rows = cursor.rowcount
            self.conn.commit()
            return rows
        except Exception as e:
            self.conn.rollback()
            raise RuntimeError(f"Upsert to {table} failed: {e}")
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Simple insert (small batches, with conflict handling)
    # ------------------------------------------------------------------

    def insert_rows(self, table: str, rows: List[Dict[str, Any]], conflict_action: str = "DO NOTHING") -> int:
        """Insert a list of dicts into a table."""
        if not rows:
            return 0

        cols = list(rows[0].keys())
        col_list = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
        values = [[row.get(c) for c in cols] for row in rows]

        cursor = self.conn.cursor()
        try:
            query_str = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s ON CONFLICT {conflict_action}"
            template = "(" + ", ".join(["%s"] * len(cols)) + ")"
            execute_values(cursor, query_str, values, template=template, page_size=500)
            rows_inserted = cursor.rowcount
            self.conn.commit()
            return rows_inserted
        except Exception as e:
            self.conn.rollback()
            raise RuntimeError(f"Insert to {table} failed: {e}")
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Extraction state management
    # ------------------------------------------------------------------

    def get_extraction_state(self, extraction_type: str) -> Dict[str, Any]:
        """Get current extraction state from DB."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT last_block_processed, total_items_processed, status, error_message, updated_at "
                "FROM extraction_state WHERE extraction_type = %s",
                (extraction_type,),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "last_block_processed": row[0],
                    "total_items_processed": row[1],
                    "status": row[2],
                    "error_message": row[3],
                    "updated_at": row[4],
                }
            return {"last_block_processed": 0, "total_items_processed": 0, "status": "idle"}
        finally:
            cursor.close()

    def update_extraction_state(
        self,
        extraction_type: str,
        last_block: int,
        total_items: int = 0,
        status: str = "running",
        error_message: Optional[str] = None,
    ) -> None:
        """Update extraction state in DB."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO extraction_state (extraction_type, last_block_processed, total_items_processed, status, error_message, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (extraction_type)
                DO UPDATE SET
                    last_block_processed = EXCLUDED.last_block_processed,
                    total_items_processed = extraction_state.total_items_processed + EXCLUDED.total_items_processed,
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    updated_at = NOW()
                """,
                (extraction_type, last_block, total_items, status, error_message),
            )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Failed to update extraction state: {e}")
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Contract discovery
    # ------------------------------------------------------------------

    def upsert_contracts(self, addresses: List[Dict[str, Any]]) -> int:
        """Upsert contract address records."""
        if not addresses:
            return 0

        cursor = self.conn.cursor()
        try:
            query = """
                INSERT INTO contracts (address, first_seen_block, last_activity_block, event_count)
                VALUES %s
                ON CONFLICT (address) DO UPDATE SET
                    last_activity_block = GREATEST(contracts.last_activity_block, EXCLUDED.last_activity_block),
                    event_count = contracts.event_count + EXCLUDED.event_count,
                    updated_at = NOW()
            """
            values = [
                (a["address"], a.get("first_seen_block", 0), a.get("last_activity_block", 0), a.get("event_count", 1))
                for a in addresses
            ]
            execute_values(cursor, query, values, template="(%s, %s, %s, %s)", page_size=500)
            rows = cursor.rowcount
            self.conn.commit()
            return rows
        except Exception as e:
            self.conn.rollback()
            raise RuntimeError(f"Contract upsert failed: {e}")
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Analytics helpers
    # ------------------------------------------------------------------

    def refresh_materialized_views(self) -> None:
        """Refresh all analytics materialized views."""
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT refresh_analytics_views()")
            self.conn.commit()
            print("Materialized views refreshed.")
        except Exception as e:
            self.conn.rollback()
            print(f"Failed to refresh views: {e}")
        finally:
            cursor.close()

    def query(self, sql_str: str, params: Optional[tuple] = None) -> List[tuple]:
        """Execute a read query and return results."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql_str, params)
            return cursor.fetchall()
        finally:
            cursor.close()

    def query_df(self, sql_str: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute a query and return results as a DataFrame."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql_str, params)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        finally:
            cursor.close()

    def get_table_counts(self) -> Dict[str, int]:
        """Get row counts for all core tables."""
        tables = ["blocks", "transactions", "raw_logs", "decoded_events", "contracts"]
        counts = {}
        cursor = self.conn.cursor()
        try:
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    counts[table] = cursor.fetchone()[0]
                except Exception:
                    counts[table] = -1
                    self.conn.rollback()
            return counts
        finally:
            cursor.close()
