"""Tests for configuration management."""

import json
import os

from mockhaus.config import HistoryConfig, MockhausConfig, get_config, reload_config


class TestHistoryConfig:
    """Test cases for HistoryConfig."""

    def test_default_values(self):
        """Test default history configuration values."""
        config = HistoryConfig()

        assert config.enabled is True
        assert config.db_path is None
        assert config.retention_days == 30
        assert config.max_size_mb == 1000
        assert config.redact_literals is False
        assert config.exclude_patterns == [".*password.*", ".*secret.*"]
        assert config.batch_size == 100
        assert config.async_write is True

    def test_from_dict(self):
        """Test creating HistoryConfig from dictionary."""
        data = {"enabled": False, "retention_days": 7, "redact_literals": True, "unknown_field": "ignored"}

        config = HistoryConfig.from_dict(data)

        assert config.enabled is False
        assert config.retention_days == 7
        assert config.redact_literals is True
        assert config.max_size_mb == 1000  # Default value


class TestMockhausConfig:
    """Test cases for MockhausConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = MockhausConfig()

        assert config.default_database is None
        assert isinstance(config.history, HistoryConfig)
        assert config.server_host == "localhost"
        assert config.server_port == 8080
        assert config.use_ast_parser is True
        assert config.enable_query_cache is False

    def test_load_from_file(self, tmp_path):
        """Test loading configuration from file."""
        config_file = tmp_path / "mockhaus.config.json"
        config_data = {"default_database": "/tmp/test.db", "server_port": 9090, "history": {"enabled": False, "retention_days": 14}}

        with open(config_file, "w") as f:
            json.dump(config_data, f)

        # Change to temp directory to load config
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = MockhausConfig.load()

            assert config.default_database == "/tmp/test.db"
            assert config.server_port == 9090
            assert config.history.enabled is False
            assert config.history.retention_days == 14
        finally:
            os.chdir(original_cwd)

    def test_load_from_env_vars(self):
        """Test loading configuration from environment variables."""
        env_vars = {
            "MOCKHAUS_DEFAULT_DATABASE": "/env/test.db",
            "MOCKHAUS_HISTORY_ENABLED": "false",
            "MOCKHAUS_HISTORY_RETENTION_DAYS": "7",
            "MOCKHAUS_SERVER_PORT": "8888",
            "MOCKHAUS_USE_AST_PARSER": "false",
        }

        # Set environment variables
        for key, value in env_vars.items():
            os.environ[key] = value

        try:
            config = MockhausConfig.load()

            assert config.default_database == "/env/test.db"
            assert config.history.enabled is False
            assert config.history.retention_days == 7
            assert config.server_port == 8888
            assert config.use_ast_parser is False
        finally:
            # Clean up env vars
            for key in env_vars:
                os.environ.pop(key, None)

    def test_env_vars_override_file(self, tmp_path):
        """Test that environment variables override file configuration."""
        # Create config file
        config_file = tmp_path / "mockhaus.config.json"
        config_data = {"server_port": 9090, "history": {"enabled": True}}

        with open(config_file, "w") as f:
            json.dump(config_data, f)

        # Set environment variable to override
        os.environ["MOCKHAUS_SERVER_PORT"] = "7777"
        os.environ["MOCKHAUS_HISTORY_ENABLED"] = "false"

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = MockhausConfig.load()

            # Env var should override file
            assert config.server_port == 7777
            assert config.history.enabled is False
        finally:
            os.chdir(original_cwd)
            os.environ.pop("MOCKHAUS_SERVER_PORT", None)
            os.environ.pop("MOCKHAUS_HISTORY_ENABLED", None)

    def test_save_config(self, tmp_path):
        """Test saving configuration to file."""
        config = MockhausConfig()
        config.default_database = "/test/save.db"
        config.server_port = 9999
        config.history.retention_days = 60

        save_path = tmp_path / "saved_config.json"
        config.save(save_path)

        # Load and verify
        with open(save_path) as f:
            data = json.load(f)

        assert data["default_database"] == "/test/save.db"
        assert data["server_port"] == 9999
        assert data["history"]["retention_days"] == 60

    def test_to_dict(self):
        """Test converting configuration to dictionary."""
        config = MockhausConfig()
        config.default_database = "/test.db"

        data = config.to_dict()

        assert isinstance(data, dict)
        assert data["default_database"] == "/test.db"
        assert isinstance(data["history"], dict)
        assert data["history"]["enabled"] is True

    def test_merge_config(self):
        """Test merging configuration data."""
        config = MockhausConfig()
        data = {"server_port": 5555, "history": {"enabled": False, "batch_size": 200}}

        merged = MockhausConfig._merge_config(config, data)

        assert merged.server_port == 5555
        assert merged.history.enabled is False
        assert merged.history.batch_size == 200
        assert merged.history.retention_days == 30  # Default unchanged

    def test_set_nested(self):
        """Test setting nested attributes."""
        config = MockhausConfig()

        MockhausConfig._set_nested(config, "history.enabled", False)
        assert config.history.enabled is False

        MockhausConfig._set_nested(config, "history.retention_days", 90)
        assert config.history.retention_days == 90


class TestGlobalConfig:
    """Test global configuration functions."""

    def test_get_config(self):
        """Test getting global configuration instance."""
        config1 = get_config()
        config2 = get_config()

        # Should return same instance
        assert config1 is config2

    def test_reload_config(self):
        """Test reloading configuration."""
        # Set an env var
        os.environ["MOCKHAUS_SERVER_PORT"] = "4444"

        try:
            # First load
            config1 = get_config()

            # Change env var
            os.environ["MOCKHAUS_SERVER_PORT"] = "5555"

            # Reload
            config2 = reload_config()

            # Should be new instance with new value
            assert config2 is not config1
            assert config2.server_port == 5555

            # get_config should now return new instance
            config3 = get_config()
            assert config3 is config2
        finally:
            os.environ.pop("MOCKHAUS_SERVER_PORT", None)
