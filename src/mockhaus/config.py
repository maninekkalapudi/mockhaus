"""Configuration management for Mockhaus."""

import json
import os
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class HistoryConfig:
    """Configuration for query history."""

    enabled: bool = True
    db_path: str | None = None  # None means use default ~/.mockhaus/history.duckdb
    retention_days: int = 30
    max_size_mb: int = 1000

    # Privacy settings
    redact_literals: bool = False
    exclude_patterns: list[str] = field(default_factory=lambda: [".*password.*", ".*secret.*"])

    # Performance settings
    batch_size: int = 100
    async_write: bool = True

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HistoryConfig":
        """Create HistoryConfig from dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


@dataclass
class MockhausConfig:
    """Main configuration for Mockhaus."""

    # Database settings
    default_database: str | None = None

    # History settings
    history: HistoryConfig = field(default_factory=HistoryConfig)

    # Server settings
    server_host: str = "localhost"
    server_port: int = 8080

    # Feature flags
    use_ast_parser: bool = True
    enable_query_cache: bool = False

    @classmethod
    def load(cls) -> "MockhausConfig":
        """Load configuration from various sources."""
        config = cls()

        # 1. Load from config file if exists
        config_paths = [Path.home() / ".mockhaus" / "config.json", Path.cwd() / ".mockhaus.json", Path.cwd() / "mockhaus.config.json"]

        for config_path in config_paths:
            if config_path.exists():
                try:
                    with open(config_path) as f:
                        data = json.load(f)
                        config = cls._merge_config(config, data)
                        break
                except Exception:
                    pass

        # 2. Override with environment variables
        env_mappings: dict[str, str | tuple[str, Callable[[str], Any]]] = {
            "MOCKHAUS_DEFAULT_DATABASE": "default_database",
            "MOCKHAUS_HISTORY_ENABLED": ("history.enabled", lambda x: x.lower() == "true"),
            "MOCKHAUS_HISTORY_DB_PATH": "history.db_path",
            "MOCKHAUS_HISTORY_RETENTION_DAYS": ("history.retention_days", int),
            "MOCKHAUS_SERVER_HOST": "server_host",
            "MOCKHAUS_SERVER_PORT": ("server_port", int),
            "MOCKHAUS_USE_AST_PARSER": ("use_ast_parser", lambda x: x.lower() == "true"),
        }

        for env_var, config_mapping in env_mappings.items():
            value = os.environ.get(env_var)
            if value is not None:
                if isinstance(config_mapping, tuple):
                    path, converter = config_mapping
                    converted_value = converter(value)
                    config = cls._set_nested(config, path, converted_value)
                else:
                    setattr(config, config_mapping, value)

        return config

    @classmethod
    def _merge_config(cls, config: "MockhausConfig", data: dict[str, Any]) -> "MockhausConfig":
        """Merge configuration data into config object."""
        if "history" in data and isinstance(data["history"], dict):
            config.history = HistoryConfig.from_dict(data["history"])
            data = {k: v for k, v in data.items() if k != "history"}

        for key, value in data.items():
            if hasattr(config, key):
                setattr(config, key, value)

        return config

    @classmethod
    def _set_nested(cls, config: "MockhausConfig", path: str, value: Any) -> "MockhausConfig":
        """Set a nested attribute using dot notation."""
        parts = path.split(".")
        obj = config
        for part in parts[:-1]:
            obj = getattr(obj, part)
        setattr(obj, parts[-1], value)
        return config

    def save(self, path: Path | None = None) -> None:
        """Save configuration to file."""
        if path is None:
            config_dir = Path.home() / ".mockhaus"
            config_dir.mkdir(exist_ok=True)
            path = config_dir / "config.json"

        data = asdict(self)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        return asdict(self)


# Global config instance
_config: MockhausConfig | None = None


def get_config() -> MockhausConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = MockhausConfig.load()
    return _config


def reload_config() -> MockhausConfig:
    """Reload configuration from sources."""
    global _config
    _config = MockhausConfig.load()
    return _config
