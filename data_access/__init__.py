"""
Data Access Layer for LLM_JUDGE Pipeline

This module provides an abstraction layer for database operations,
enabling the pipeline to run in multiple environments:

1. SNOWPARK MODE: Direct Snowpark session (running inside Snowflake)
2. CONNECTOR MODE: External snowflake-connector-python (GitHub Actions, Colab)

Usage:
    # Create appropriate data access based on environment
    from data_access import create_data_access, DataAccessInterface
    
    data_access = create_data_access(config)
    
    # Use uniform interface
    conversations = data_access.read_conversations(table, date, department)
    data_access.write_results(results_df, output_table)
"""

from data_access.interface import (
    DataAccessInterface,
    DataAccessConfig,
    TableConfig,
)
from data_access.factory import (
    create_data_access,
    detect_data_access_mode,
    DataAccessMode,
)

__all__ = [
    # Interface
    'DataAccessInterface',
    'DataAccessConfig',
    'TableConfig',
    # Factory
    'create_data_access',
    'detect_data_access_mode',
    'DataAccessMode',
]
