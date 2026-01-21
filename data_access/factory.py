"""
Data Access Factory

Creates appropriate data access implementation based on environment
and configuration.

Lessons Learned from Colab Deployment (incorporated):
1. Module shadowing: Check for real snowflake.connector before assuming Snowpark
2. Environment detection should be robust to partial imports
"""

import os
import sys
from enum import Enum
from typing import Dict, Any, Optional, Union

from data_access.interface import DataAccessInterface, DataAccessConfig


class DataAccessMode(Enum):
    """Available data access modes."""
    SNOWPARK = "snowpark"        # Running inside Snowflake with Snowpark session
    CONNECTOR = "connector"       # Running externally with snowflake-connector-python
    AUTO = "auto"                 # Auto-detect based on environment


def detect_data_access_mode() -> DataAccessMode:
    """
    Detect the appropriate data access mode based on environment.
    
    Detection logic:
    1. If running inside Snowflake (Snowpark available and functional) -> SNOWPARK
    2. If running externally with connector available -> CONNECTOR
    3. Check for module shadowing issues (local snowflake/ folder)
    
    Returns:
        DataAccessMode.SNOWPARK or DataAccessMode.CONNECTOR
    """
    # First, check for module shadowing issue
    # (lesson learned from Colab deployment)
    if 'snowflake' in sys.modules:
        sf_module = sys.modules['snowflake']
        module_file = getattr(sf_module, '__file__', '')
        if module_file and 'site-packages' not in module_file:
            # Local snowflake module shadowing installed package
            # This is external mode with shadowing issue
            return DataAccessMode.CONNECTOR
    
    # Try Snowpark first
    try:
        import snowflake.snowpark as snowpark
        from snowflake.snowpark import Session
        
        # Additional check: can we actually use Snowpark?
        # Inside Snowflake, Snowpark is functional
        # Outside, import may succeed but usage fails
        if hasattr(snowpark, 'Session') and snowpark.Session is not None:
            return DataAccessMode.SNOWPARK
    except ImportError:
        pass
    except Exception:
        # Any other error means Snowpark isn't functional
        pass
    
    # Check for connector
    try:
        import snowflake.connector
        if hasattr(snowflake.connector, 'connect'):
            return DataAccessMode.CONNECTOR
    except ImportError:
        pass
    
    # Default to connector (will fail later if not available)
    return DataAccessMode.CONNECTOR


def create_data_access(
    mode: DataAccessMode = DataAccessMode.AUTO,
    config: Optional[DataAccessConfig] = None,
    snowpark_session: Any = None,
    connection_config: Optional[Dict[str, str]] = None
) -> DataAccessInterface:
    """
    Create appropriate data access implementation.
    
    Args:
        mode: Data access mode (AUTO, SNOWPARK, or CONNECTOR)
        config: DataAccessConfig object with settings
        snowpark_session: Optional Snowpark session (for SNOWPARK mode)
        connection_config: Optional connection dict (for CONNECTOR mode)
    
    Returns:
        DataAccessInterface implementation
    
    Usage Examples:
    
    # Auto-detect mode (recommended for portable code)
    data_access = create_data_access()
    
    # Explicit Snowpark mode (inside Snowflake)
    data_access = create_data_access(
        mode=DataAccessMode.SNOWPARK,
        snowpark_session=session
    )
    
    # Explicit Connector mode (external)
    data_access = create_data_access(
        mode=DataAccessMode.CONNECTOR,
        connection_config={
            'account': '...',
            'user': '...',
            'password': '...',
            'warehouse': 'LLMS_WH',
            'database': 'LLM_EVAL',
            'schema': 'PUBLIC',
            'role': 'LLM_ROLE'
        }
    )
    
    # From environment variables
    data_access = create_data_access(
        mode=DataAccessMode.CONNECTOR,
        config=DataAccessConfig.from_env()
    )
    """
    # Auto-detect mode if needed
    if mode == DataAccessMode.AUTO:
        mode = detect_data_access_mode()
    
    # Build config if not provided
    if config is None:
        config = DataAccessConfig()
    
    # Add snowpark session to config if provided
    if snowpark_session is not None:
        config.snowpark_session = snowpark_session
    
    # Add connection details to config if provided
    if connection_config:
        config.account = connection_config.get('account', config.account)
        config.user = connection_config.get('user', config.user)
        config.password = connection_config.get('password', config.password)
        config.warehouse = connection_config.get('warehouse', config.warehouse)
        config.database = connection_config.get('database', config.database)
        config.schema = connection_config.get('schema', config.schema)
        config.role = connection_config.get('role', config.role)
    
    # Create appropriate implementation
    if mode == DataAccessMode.SNOWPARK:
        from data_access.snowpark_access import SnowparkDataAccess
        
        if config.snowpark_session is None:
            raise ValueError(
                "Snowpark mode requires a snowpark_session. "
                "Either pass it directly or use AUTO mode."
            )
        
        return SnowparkDataAccess(config.snowpark_session, config)
    
    elif mode == DataAccessMode.CONNECTOR:
        from data_access.connector_access import ConnectorDataAccess
        
        # Try to load from environment if not in config
        if not config.is_connector_mode():
            config = _load_config_from_env(config)
        
        if not config.is_connector_mode():
            raise ValueError(
                "Connector mode requires account, user, and password. "
                "Either provide them in config or set environment variables: "
                "SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD"
            )
        
        return ConnectorDataAccess(config)
    
    else:
        raise ValueError(f"Unknown data access mode: {mode}")


def _load_config_from_env(config: DataAccessConfig) -> DataAccessConfig:
    """Load configuration from environment variables."""
    config.account = os.getenv('SNOWFLAKE_ACCOUNT', config.account)
    config.user = os.getenv('SNOWFLAKE_USER', config.user)
    config.password = os.getenv('SNOWFLAKE_PASSWORD', config.password)
    config.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', config.warehouse or 'LLMS_WH')
    config.database = os.getenv('SNOWFLAKE_DATABASE', config.database or 'LLM_EVAL')
    config.schema = os.getenv('SNOWFLAKE_SCHEMA', config.schema or 'PUBLIC')
    config.role = os.getenv('SNOWFLAKE_ROLE', config.role or 'LLM_ROLE')
    
    return config


def create_data_access_from_env() -> DataAccessInterface:
    """
    Create data access using environment variables.
    
    Convenience function for GitHub Actions and Colab where
    credentials are stored in environment variables.
    
    Required environment variables:
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_USER
    - SNOWFLAKE_PASSWORD
    
    Optional environment variables:
    - SNOWFLAKE_WAREHOUSE (default: LLMS_WH)
    - SNOWFLAKE_DATABASE (default: LLM_EVAL)
    - SNOWFLAKE_SCHEMA (default: PUBLIC)
    - SNOWFLAKE_ROLE (default: LLM_ROLE)
    
    Returns:
        DataAccessInterface (ConnectorDataAccess)
    """
    config = DataAccessConfig()
    config = _load_config_from_env(config)
    
    return create_data_access(
        mode=DataAccessMode.CONNECTOR,
        config=config
    )
