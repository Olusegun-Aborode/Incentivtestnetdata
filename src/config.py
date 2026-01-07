import os
import re
from pathlib import Path
from typing import Any, Dict

import yaml


ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        def replace(match: re.Match) -> str:
            key = match.group(1)
            return os.environ.get(key, "")

        return ENV_PATTERN.sub(replace, value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def load_yaml(path: str) -> Dict[str, Any]:
    data = yaml.safe_load(Path(path).read_text())
    return _expand_env(data)
