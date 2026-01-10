"""
Event router for mapping events to their respective Dune tables.
"""

from typing import Dict, List

# Map event names to table names
EVENT_TABLE_MAP = {
    # DEX Pool events
    'Swap': 'dex_events',
    'Mint': 'dex_events',
    'Burn': 'dex_events',
    'Collect': 'dex_events',
    'CollectProtocol': 'dex_events',
    'Flash': 'dex_events',
    'Initialize': 'dex_events',
    'SetFeeProtocol': 'dex_events',
    'IncreaseObservationCardinalityNext': 'dex_events',
    
    # Hyperlane Mailbox events
    'Dispatch': 'hyperlane_events',
    'DispatchId': 'hyperlane_events',
    'Process': 'hyperlane_events',
    'ProcessId': 'hyperlane_events',
    'DefaultHookSet': 'hyperlane_events',
    'DefaultIsmSet': 'hyperlane_events',
    'RequiredHookSet': 'hyperlane_events',
    'Initialized': 'hyperlane_events',
    'OwnershipTransferred': 'hyperlane_events',
    
    # Warp Route events
    'ReceivedTransferRemote': 'warp_route_events',
    'SentTransferRemote': 'warp_route_events',
    'GasSet': 'warp_route_events',
    'HookSet': 'warp_route_events',
    'IsmSet': 'warp_route_events',
    
    # ERC20 events
    'Transfer': 'erc20_events',
    'Approval': 'erc20_events',
}

# Define schema for each table (columns in order)
TABLE_SCHEMAS = {
    'dex_events': [
        'block_number', 'block_timestamp', 'tx_hash', 'log_index', 'address',
        'event_name', 'chain', 'extracted_at',
        # Event-specific fields
        'sender', 'recipient', 'owner', 'amount', 'amount0', 'amount1',
        'tick_lower', 'tick_upper', 'sqrt_price_x96', 'liquidity', 'tick',
        'paid0', 'paid1', 'fee_protocol0_old', 'fee_protocol1_old',
        'fee_protocol0_new', 'fee_protocol1_new',
        'observation_cardinality_next_old', 'observation_cardinality_next_new'
    ],
    
    'warp_route_events': [
        'block_number', 'block_timestamp', 'tx_hash', 'log_index', 'address',
        'event_name', 'chain', 'extracted_at',
        # Event-specific fields
        'origin', 'destination', 'recipient', 'amount', 'domain', 'gas',
        'hook', 'ism'
    ],
    
    'hyperlane_events': [
        'block_number', 'block_timestamp', 'tx_hash', 'log_index', 'address',
        'event_name', 'chain', 'extracted_at',
        # Event-specific fields
        'sender', 'destination', 'recipient', 'message', 'message_id',
        'origin', 'hook', 'module', 'version', 'previous_owner', 'new_owner'
    ],
    
    'erc20_events': [
        'block_number', 'block_timestamp', 'tx_hash', 'log_index', 'address',
        'event_name', 'chain', 'extracted_at',
        # Event-specific fields
        'from', 'to', 'owner', 'spender', 'value'
    ],
}


def get_table_for_event(event_name: str) -> str:
    """Get the Dune table name for a given event."""
    return EVENT_TABLE_MAP.get(event_name)


def get_schema_for_table(table_name: str) -> List[str]:
    """Get the column schema for a given table."""
    return TABLE_SCHEMAS.get(table_name, [])


def get_all_tables() -> List[str]:
    """Get list of all table names."""
    return list(set(EVENT_TABLE_MAP.values()))
