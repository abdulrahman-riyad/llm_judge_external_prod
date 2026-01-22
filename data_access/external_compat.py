"""
External Compatibility Layer for Metrics Calculation

This module provides functions that mirror snowflake_llm_processor functions
but work with snowflake.connector instead of snowflake.snowpark.

This allows metrics calculation to run in GitHub Actions and Colab
without requiring snowflake-snowpark-python.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
import json

# Global connection - set by run_pipeline.py
_global_connection = None

def set_global_connection(conn):
    """Set the global Snowflake connection for use by compatibility functions."""
    global _global_connection
    _global_connection = conn

def get_global_connection():
    """Get the global Snowflake connection."""
    global _global_connection
    if _global_connection is None:
        raise RuntimeError("Global connection not set. Call set_global_connection() first.")
    return _global_connection


def insert_raw_data_with_cleanup(
    session,  # Can be Snowpark session OR snowflake.connector connection
    table_name: str, 
    department: str, 
    target_date, 
    dataframe: pd.DataFrame, 
    columns: list, 
    additional_filter: dict = None
) -> dict:
    """
    External-compatible version of insert_raw_data_with_cleanup.
    
    Works with both Snowpark sessions and snowflake.connector connections.
    
    Args:
        session: Snowflake session (Snowpark) or connection (connector)
        table_name: Name of the target table
        department: Department value to add to all rows
        target_date: Target date for the data
        dataframe: Pandas dataframe containing the data to insert
        columns: List of column names that should match dataframe columns
        additional_filter: Optional dict with column-value pairs for more granular deletion
        
    Returns:
        dict: Summary of the operation
    """
    try:
        # Detect if we're using Snowpark or connector
        is_snowpark = hasattr(session, 'create_dataframe')
        
        if is_snowpark:
            # Use original Snowpark implementation
            return _insert_with_snowpark(session, table_name, department, target_date, 
                                         dataframe, columns, additional_filter)
        else:
            # Use connector implementation
            return _insert_with_connector(session, table_name, department, target_date,
                                          dataframe, columns, additional_filter)
    except Exception as e:
        print(f"❌ Error in insert_raw_data_with_cleanup: {e}")
        return {'success': False, 'error': str(e), 'rows_inserted': 0}


def _insert_with_connector(
    conn,
    table_name: str,
    department: str,
    target_date,
    dataframe: pd.DataFrame,
    columns: list,
    additional_filter: dict = None
) -> dict:
    """Insert data using snowflake.connector."""
    from snowflake.connector.pandas_tools import write_pandas
    
    try:
        # Handle both raw connection and DataAccessInterface wrappers
        # If conn has _connection attribute, it's a wrapper (like ConnectorDataAccess)
        if hasattr(conn, '_connection'):
            actual_conn = conn._connection
            # Get default database/schema from wrapper config
            default_db = getattr(conn.config, 'database', None) if hasattr(conn, 'config') else None
            default_schema = getattr(conn.config, 'schema', None) if hasattr(conn, 'config') else None
        elif hasattr(conn, 'connection'):
            actual_conn = conn.connection
            default_db = None
            default_schema = None
        else:
            actual_conn = conn
            default_db = None
            default_schema = None
        
        cursor = actual_conn.cursor()
        
        # CRITICAL: Set database and schema context for the session
        # This fixes "Cannot perform CREATE TABLE. This session does not have a current database"
        # Use defaults from config or hardcoded fallbacks
        db_to_use = default_db or 'LLM_EVAL'
        schema_to_use = default_schema or 'PUBLIC'
        
        try:
            cursor.execute(f"USE DATABASE {db_to_use}")
            cursor.execute(f"USE SCHEMA {schema_to_use}")
        except Exception as ctx_err:
            print(f"⚠️ Could not set database context: {ctx_err}")
        
        # Step 1: Check if table exists
        try:
            check_query = f"""
            SELECT COUNT(*) AS count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = UPPER('{table_name.split('.')[-1]}')
            """
            cursor.execute(check_query)
            result = cursor.fetchone()
            exists = result[0] > 0 if result else False
        except:
            exists = False
        
        # Step 2: Create table if it doesn't exist
        if not exists:
            essential_cols = {
                'DATE': 'DATE',
                'DEPARTMENT': 'VARCHAR(100)',
                'TIMESTAMP': 'TIMESTAMP'
            }
            dynamic_cols = {col_name: 'VARCHAR(16777216)' for col_name in columns}
            full_schema = {**essential_cols, **dynamic_cols}
            create_cols_str = ",\n    ".join([f"{col} {dtype}" for col, dtype in full_schema.items()])
            create_query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {create_cols_str}\n)"
            cursor.execute(create_query)
            print(f"✅ Created table {table_name}")
        
        # Step 3: Calculate current timestamp
        current_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Step 4: Delete existing rows
        delete_conditions = [f"DATE = '{target_date}'", f"DEPARTMENT = '{department}'"]
        if additional_filter:
            for col, val in additional_filter.items():
                safe_val = str(val).replace("'", "''")
                delete_conditions.append(f"{col} = '{safe_val}'")
        
        delete_where_clause = " AND ".join(delete_conditions)
        delete_query = f"DELETE FROM {table_name} WHERE {delete_where_clause}"
        cursor.execute(delete_query)
        actual_conn.commit()
        
        # Step 5: Prepare dataframe
        dataframe_copy = dataframe.copy()
        dataframe_copy['DATE'] = target_date
        dataframe_copy['TIMESTAMP'] = current_ts
        dataframe_copy['DEPARTMENT'] = department
        
        # Reorder columns
        essential_cols = ['DATE', 'DEPARTMENT', 'TIMESTAMP']
        final_column_order = essential_cols + columns
        
        # Only include columns that exist in dataframe
        available_cols = [c for c in final_column_order if c in dataframe_copy.columns]
        dataframe_copy = dataframe_copy[available_cols]
        
        # Step 6: Write using write_pandas
        # Parse table name to get db/schema/table parts
        parts = table_name.split('.')
        if len(parts) == 3:
            db, schema, tbl = parts
        elif len(parts) == 2:
            db = db_to_use  # Use default database
            schema, tbl = parts
        else:
            db = db_to_use  # Use default database
            schema = schema_to_use  # Use default schema
            tbl = table_name
        
        success, nchunks, nrows, _ = write_pandas(
            conn=actual_conn,
            df=dataframe_copy,
            table_name=tbl,
            database=db,
            schema=schema,
            auto_create_table=False
        )
        
        cursor.close()
        
        return {
            'success': success,
            'rows_inserted': nrows,
            'table': table_name
        }
        
    except Exception as e:
        print(f"❌ Connector insert error: {e}")
        return {'success': False, 'error': str(e), 'rows_inserted': 0}


def _insert_with_snowpark(session, table_name, department, target_date, 
                          dataframe, columns, additional_filter):
    """Original Snowpark implementation - delegates to snowflake_llm_processor."""
    # Import here to avoid circular imports and only when Snowpark is available
    from snowflake_llm_processor import insert_raw_data_with_cleanup as snowpark_insert
    return snowpark_insert(session, table_name, department, target_date, 
                           dataframe, columns, additional_filter)


def clean_dataframe_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a DataFrame for safe insertion into Snowflake.
    
    Handles:
    - NaN/None values
    - Special characters in strings
    - Data type conversions
    """
    df_clean = df.copy()
    
    for col in df_clean.columns:
        # Convert to string for text columns, handling None/NaN
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].fillna('')
            df_clean[col] = df_clean[col].astype(str)
            # Replace problematic characters
            df_clean[col] = df_clean[col].str.replace('\x00', '', regex=False)
    
    return df_clean


# Alias for compatibility
def process_department_phase1(*args, **kwargs):
    """
    Stub for process_department_phase1.
    
    This function is used for total_chats calculation but requires
    full Snowpark functionality. In external mode, we return empty results.
    
    Returns a tuple that can be unpacked: (filtered_df, phase1_stats, success)
    """
    print("⚠️ process_department_phase1 not available in external mode")
    # Return empty DataFrame, empty stats dict, and False to indicate no data
    return pd.DataFrame(), {}, False
