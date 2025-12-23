"""
Property-based tests for configuration loading.

**Feature: memory-price-monitor, Property 6: Configuration reload consistency**
**Validates: Requirements 2.5**
"""

import pytest
import json
import tempfile
import os
from pathlib import Path
from hypothesis import given, strategies as st
from hypothesis import settings

# Add project root to path
import sys
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import ConfigManager, SystemConfig, DatabaseConfig, NotificationConfig


# Hypothesis strategies for generating test data
@st.composite
def database_config_strategy(draw):
    """Generate valid database configuration."""
    return {
        "host": draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'), whitelist_characters='.-'))),
        "port": draw(st.integers(min_value=1, max_value=65535)),
        "database": draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'), whitelist_characters='_-'))),
        "username": draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'), whitelist_characters='_-'))),
        "password": draw(st.text(max_size=100)),
        "pool_size": draw(st.integers(min_value=1, max_value=100)),
        "max_overflow": draw(st.integers(min_value=0, max_value=200))
    }


@st.composite
def notification_config_strategy(draw):
    """Generate valid notification configuration."""
    return {
        "wechat_method": draw(st.sampled_from(["serverchan", "wxpy"])),
        "serverchan_key": draw(st.one_of(st.none(), st.text(min_size=10, max_size=100))),
        "email_smtp_host": draw(st.text(min_size=1, max_size=100, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'), whitelist_characters='.-'))),
        "email_smtp_port": draw(st.integers(min_value=1, max_value=65535)),
        "email_username": draw(st.one_of(st.none(), st.text(min_size=1, max_size=100))),
        "email_password": draw(st.one_of(st.none(), st.text(max_size=100))),
        "email_recipients": draw(st.lists(st.text(min_size=5, max_size=100, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'), whitelist_characters='@.-_')), max_size=10)),
        "retry_attempts": draw(st.integers(min_value=1, max_value=10)),
        "retry_delay": draw(st.floats(min_value=0.1, max_value=60.0))
    }


@st.composite
def system_config_strategy(draw):
    """Generate valid system configuration."""
    return {
        "database": draw(database_config_strategy()),
        "notification": draw(notification_config_strategy()),
        "log_level": draw(st.sampled_from(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])),
        "data_retention_days": draw(st.integers(min_value=1, max_value=3650))
    }


class TestConfigurationReloadConsistency:
    """Test configuration reload consistency property."""
    
    @given(config_data=system_config_strategy())
    @settings(max_examples=100)
    def test_configuration_reload_consistency(self, config_data):
        """
        **Feature: memory-price-monitor, Property 6: Configuration reload consistency**
        
        For any configuration change, reloading settings should update system behavior 
        without requiring restart and without affecting ongoing operations.
        
        **Validates: Requirements 2.5**
        """
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(config_data, temp_file, indent=2)
            temp_config_path = temp_file.name
        
        try:
            # Create config manager with the temporary file
            config_manager = ConfigManager(temp_config_path)
            
            # Load initial configuration
            initial_config = config_manager.load_config()
            
            # Verify initial config matches expected values
            assert initial_config.database.host == config_data["database"]["host"]
            assert initial_config.database.port == config_data["database"]["port"]
            assert initial_config.notification.wechat_method == config_data["notification"]["wechat_method"]
            assert initial_config.log_level == config_data["log_level"]
            
            # Modify the configuration file
            modified_config_data = config_data.copy()
            modified_config_data["log_level"] = "ERROR" if config_data["log_level"] != "ERROR" else "DEBUG"
            modified_config_data["data_retention_days"] = config_data["data_retention_days"] + 10
            
            # Write modified configuration
            with open(temp_config_path, 'w') as f:
                json.dump(modified_config_data, f, indent=2)
            
            # Force reload by clearing cache
            config_manager._config = None
            config_manager._last_modified = None
            
            # Load updated configuration
            updated_config = config_manager.load_config()
            
            # Verify configuration was reloaded with new values
            assert updated_config.log_level == modified_config_data["log_level"]
            assert updated_config.data_retention_days == modified_config_data["data_retention_days"]
            
            # Verify other values remain consistent
            assert updated_config.database.host == config_data["database"]["host"]
            assert updated_config.database.port == config_data["database"]["port"]
            assert updated_config.notification.wechat_method == config_data["notification"]["wechat_method"]
            
            # Test automatic reload detection
            config_manager._config = initial_config  # Reset to initial
            
            # Modify file again
            modified_config_data["data_retention_days"] = config_data["data_retention_days"] + 20
            with open(temp_config_path, 'w') as f:
                json.dump(modified_config_data, f, indent=2)
            
            # Check if reload is detected
            reload_detected = config_manager.reload_if_changed()
            assert reload_detected == True
            
            # Verify the change was applied
            current_config = config_manager.load_config()
            assert current_config.data_retention_days == modified_config_data["data_retention_days"]
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_config_path):
                os.unlink(temp_config_path)
    
    @given(config_data=system_config_strategy())
    @settings(max_examples=10)
    def test_configuration_consistency_across_reloads(self, config_data):
        """
        Test that configuration remains consistent across multiple reloads
        when the file hasn't changed.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(config_data, temp_file, indent=2)
            temp_config_path = temp_file.name
        
        try:
            config_manager = ConfigManager(temp_config_path)
            
            # Load configuration multiple times
            configs = []
            for _ in range(5):
                config = config_manager.load_config()
                configs.append(config)
            
            # All configurations should be identical
            first_config = configs[0]
            for config in configs[1:]:
                assert config.database.host == first_config.database.host
                assert config.database.port == first_config.database.port
                assert config.notification.wechat_method == first_config.notification.wechat_method
                assert config.log_level == first_config.log_level
                assert config.data_retention_days == first_config.data_retention_days
                
        finally:
            if os.path.exists(temp_config_path):
                os.unlink(temp_config_path)
    
    def test_configuration_reload_with_invalid_json(self):
        """Test that configuration reload handles invalid JSON gracefully."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            # Write valid JSON first
            valid_config = {"log_level": "INFO", "data_retention_days": 365}
            json.dump(valid_config, temp_file, indent=2)
            temp_config_path = temp_file.name
        
        try:
            config_manager = ConfigManager(temp_config_path)
            
            # Load valid configuration
            initial_config = config_manager.load_config()
            assert initial_config.log_level == "INFO"
            
            # Write invalid JSON
            with open(temp_config_path, 'w') as f:
                f.write('{"invalid": json}')  # Invalid JSON
            
            # Force reload attempt
            config_manager._config = None
            config_manager._last_modified = None
            
            # Should fall back to default configuration
            fallback_config = config_manager.load_config()
            assert isinstance(fallback_config, SystemConfig)
            # Should use default values when JSON is invalid
            assert fallback_config.log_level == "INFO"  # Default value
            
        finally:
            if os.path.exists(temp_config_path):
                os.unlink(temp_config_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])