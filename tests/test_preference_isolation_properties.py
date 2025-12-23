"""
Property-based tests for preference isolation.

**Feature: memory-price-monitor, Property 18: Preference isolation**
**Validates: Requirements 5.5**
"""

import pytest
import json
import tempfile
import os
import threading
import time
from pathlib import Path
from hypothesis import given, strategies as st
from hypothesis import settings

# Add project root to path
import sys
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import ConfigManager, SystemConfig, NotificationConfig


# Hypothesis strategies for generating test data
@st.composite
def user_preference_strategy(draw):
    """Generate valid user preferences."""
    return {
        "notification_enabled": draw(st.booleans()),
        "preferred_format": draw(st.sampled_from(["text", "html", "markdown"])),
        "frequency": draw(st.sampled_from(["daily", "weekly", "monthly"])),
        "include_charts": draw(st.booleans()),
        "max_items": draw(st.integers(min_value=1, max_value=100)),
        "custom_filters": draw(st.lists(st.text(min_size=1, max_size=20), max_size=5))
    }


@st.composite
def system_config_strategy(draw):
    """Generate valid system configuration."""
    return {
        "notification": {
            "wechat_method": draw(st.sampled_from(["serverchan", "wxpy"])),
            "email_smtp_host": "smtp.gmail.com",
            "email_smtp_port": 587,
            "retry_attempts": draw(st.integers(min_value=1, max_value=5)),
            "retry_delay": draw(st.floats(min_value=1.0, max_value=10.0))
        },
        "log_level": draw(st.sampled_from(["DEBUG", "INFO", "WARNING", "ERROR"])),
        "data_retention_days": draw(st.integers(min_value=30, max_value=365))
    }


class TestPreferenceIsolation:
    """Test preference isolation property."""
    
    @given(
        user_prefs1=user_preference_strategy(),
        user_prefs2=user_preference_strategy(),
        system_config=system_config_strategy()
    )
    @settings(max_examples=10)
    def test_preference_isolation(self, user_prefs1, user_prefs2, system_config):
        """
        **Feature: memory-price-monitor, Property 18: Preference isolation**
        
        For any user preference change, updating notification settings should not 
        affect data collection operations.
        
        **Validates: Requirements 5.5**
        """
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(system_config, temp_file, indent=2)
            temp_config_path = temp_file.name
        
        try:
            config_manager = ConfigManager(temp_config_path)
            
            # Load initial configuration
            initial_config = config_manager.load_config()
            
            # Store original system configuration values
            original_log_level = initial_config.log_level
            original_retention_days = initial_config.data_retention_days
            original_wechat_method = initial_config.notification.wechat_method
            original_retry_attempts = initial_config.notification.retry_attempts
            
            # Set user preferences for multiple users
            user1_id = "user1"
            user2_id = "user2"
            
            for key, value in user_prefs1.items():
                config_manager.set_user_preference(user1_id, key, value)
            
            for key, value in user_prefs2.items():
                config_manager.set_user_preference(user2_id, key, value)
            
            # Reload configuration to ensure preferences are applied
            updated_config = config_manager.load_config()
            
            # Verify that system configuration remains unchanged
            assert updated_config.log_level == original_log_level
            assert updated_config.data_retention_days == original_retention_days
            assert updated_config.notification.wechat_method == original_wechat_method
            assert updated_config.notification.retry_attempts == original_retry_attempts
            
            # Verify that user preferences are properly isolated
            user1_stored_prefs = config_manager.get_user_preferences(user1_id)
            user2_stored_prefs = config_manager.get_user_preferences(user2_id)
            
            # User 1 preferences should match what was set
            for key, value in user_prefs1.items():
                assert user1_stored_prefs[key] == value
            
            # User 2 preferences should match what was set
            for key, value in user_prefs2.items():
                assert user2_stored_prefs[key] == value
            
            # Verify preferences are isolated (user1 changes don't affect user2)
            config_manager.set_user_preference(user1_id, "test_isolation", "user1_value")
            config_manager.set_user_preference(user2_id, "test_isolation", "user2_value")
            
            assert config_manager.get_user_preference(user1_id, "test_isolation") == "user1_value"
            assert config_manager.get_user_preference(user2_id, "test_isolation") == "user2_value"
            
            # Verify that removing one user's preferences doesn't affect the other
            config_manager.remove_user_preferences(user1_id)
            
            # User1 preferences should be gone
            assert config_manager.get_user_preferences(user1_id) == {}
            
            # User2 preferences should remain intact
            user2_remaining_prefs = config_manager.get_user_preferences(user2_id)
            for key, value in user_prefs2.items():
                assert user2_remaining_prefs[key] == value
            assert user2_remaining_prefs["test_isolation"] == "user2_value"
            
            # System configuration should still be unchanged
            final_config = config_manager.load_config()
            assert final_config.log_level == original_log_level
            assert final_config.data_retention_days == original_retention_days
            assert final_config.notification.wechat_method == original_wechat_method
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_config_path):
                os.unlink(temp_config_path)
    
    @given(
        user_prefs=user_preference_strategy(),
        system_config=system_config_strategy()
    )
    @settings(max_examples=10)
    def test_preference_persistence_across_reloads(self, user_prefs, system_config):
        """
        Test that user preferences persist across configuration reloads
        without affecting system configuration.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(system_config, temp_file, indent=2)
            temp_config_path = temp_file.name
        
        try:
            config_manager = ConfigManager(temp_config_path)
            
            # Load initial configuration and set user preferences
            initial_config = config_manager.load_config()
            user_id = "test_user"
            
            for key, value in user_prefs.items():
                config_manager.set_user_preference(user_id, key, value)
            
            # Store original system values
            original_log_level = initial_config.log_level
            original_retention_days = initial_config.data_retention_days
            
            # Modify system configuration file
            modified_system_config = system_config.copy()
            modified_system_config["log_level"] = "ERROR" if system_config["log_level"] != "ERROR" else "DEBUG"
            modified_system_config["data_retention_days"] = system_config["data_retention_days"] + 10
            
            with open(temp_config_path, 'w') as f:
                json.dump(modified_system_config, f, indent=2)
            
            # Force reload
            config_manager._config = None
            config_manager._last_modified = None
            reloaded_config = config_manager.load_config()
            
            # System configuration should be updated
            assert reloaded_config.log_level == modified_system_config["log_level"]
            assert reloaded_config.data_retention_days == modified_system_config["data_retention_days"]
            
            # User preferences should persist and remain isolated
            persisted_prefs = config_manager.get_user_preferences(user_id)
            for key, value in user_prefs.items():
                assert persisted_prefs[key] == value
            
        finally:
            if os.path.exists(temp_config_path):
                os.unlink(temp_config_path)
    
    @given(user_prefs=user_preference_strategy())
    @settings(max_examples=10)
    def test_concurrent_preference_operations(self, user_prefs):
        """
        Test that concurrent preference operations are thread-safe
        and don't interfere with system configuration.
        """
        config_manager = ConfigManager("nonexistent_config.json")  # Will use defaults
        
        # Load initial configuration
        initial_config = config_manager.load_config()
        original_log_level = initial_config.log_level
        
        # Define concurrent operations
        def set_user_preferences(user_id_prefix: str, prefs: dict):
            for i in range(5):
                user_id = f"{user_id_prefix}_{i}"
                for key, value in prefs.items():
                    config_manager.set_user_preference(user_id, key, f"{value}_{i}")
        
        def get_user_preferences(user_id_prefix: str):
            results = []
            for i in range(5):
                user_id = f"{user_id_prefix}_{i}"
                prefs = config_manager.get_user_preferences(user_id)
                results.append(prefs)
            return results
        
        # Run concurrent operations
        threads = []
        
        # Multiple threads setting preferences
        for thread_id in range(3):
            thread = threading.Thread(
                target=set_user_preferences,
                args=(f"thread{thread_id}", user_prefs)
            )
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify system configuration is unchanged
        final_config = config_manager.load_config()
        assert final_config.log_level == original_log_level
        
        # Verify all user preferences were set correctly
        for thread_id in range(3):
            for i in range(5):
                user_id = f"thread{thread_id}_{i}"
                stored_prefs = config_manager.get_user_preferences(user_id)
                
                for key, original_value in user_prefs.items():
                    expected_value = f"{original_value}_{i}"
                    assert stored_prefs[key] == expected_value
    
    def test_preference_isolation_with_invalid_system_config(self):
        """
        Test that user preferences remain functional even when
        system configuration is invalid.
        """
        # Create config with invalid system settings
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            # Write invalid JSON
            temp_file.write('{"invalid": "json", "missing_comma": true "error": true}')
            temp_config_path = temp_file.name
        
        try:
            config_manager = ConfigManager(temp_config_path)
            
            # Should fall back to default config
            config = config_manager.load_config()
            assert isinstance(config, SystemConfig)
            
            # User preferences should still work
            user_id = "test_user"
            config_manager.set_user_preference(user_id, "test_key", "test_value")
            
            stored_value = config_manager.get_user_preference(user_id, "test_key")
            assert stored_value == "test_value"
            
            # System should use defaults
            assert config.log_level == "INFO"  # Default value
            
        finally:
            if os.path.exists(temp_config_path):
                os.unlink(temp_config_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])