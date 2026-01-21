"""
Snowpark Data Access Implementation

Uses Snowpark session for data operations.
This is used when running inside Snowflake (e.g., via stored procedures).
"""

from typing import Dict, List, Any, Optional
import pandas as pd

from data_access.interface import DataAccessInterface, DataAccessConfig

# Conditional Snowpark import - only available inside Snowflake
try:
    import snowflake.snowpark as snowpark
    from snowflake.snowpark import Session
    SNOWPARK_AVAILABLE = True
except ImportError:
    SNOWPARK_AVAILABLE = False
    snowpark = None
    Session = None


class SnowparkDataAccess(DataAccessInterface):
    """
    Data access implementation using Snowpark Session.
    
    This is the primary implementation when running inside Snowflake
    as stored procedures or worksheets.
    """
    
    def __init__(self, session: 'snowpark.Session', config: Optional[DataAccessConfig] = None):
        """
        Initialize with Snowpark session.
        
        Args:
            session: Active Snowpark session
            config: Optional configuration (database/schema defaults)
        """
        if not SNOWPARK_AVAILABLE:
            raise RuntimeError("Snowpark not available. Use ConnectorDataAccess instead.")
        
        self.session = session
        self.config = config or DataAccessConfig()
    
    def read_conversations(
        self, 
        table: str, 
        date: str, 
        department: str,
        skill_filter: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Read conversations using Snowpark."""
        query = f"""
        SELECT 
            CONVERSATION_ID,
            MESSAGE_ID,
            SENT_BY,
            TEXT,
            MESSAGE_TYPE,
            MESSAGE_SENT_TIME,
            SKILL,
            START_DATE
        FROM {table}
        WHERE TO_DATE(START_DATE) = '{date}'
        """
        
        if skill_filter:
            query += f"\n          AND SKILL ILIKE '%{skill_filter}%'"
        
        query += "\n        ORDER BY CONVERSATION_ID, MESSAGE_SENT_TIME"
        
        if limit:
            query += f"\n        LIMIT {limit}"
        
        return self.session.sql(query).to_pandas()
    
    def write_results(
        self, 
        df: pd.DataFrame, 
        table: str,
        mode: str = 'append'
    ) -> int:
        """Write results using Snowpark write_pandas."""
        # Clean DataFrame for Snowflake compatibility
        df_clean = self._clean_dataframe(df)
        
        # Create Snowpark DataFrame and write
        snowpark_df = self.session.create_dataframe(df_clean)
        
        write_mode = 'append' if mode == 'append' else 'overwrite'
        snowpark_df.write.mode(write_mode).save_as_table(table)
        
        return len(df_clean)
    
    def execute_sql(self, query: str) -> pd.DataFrame:
        """Execute SQL using Snowpark."""
        return self.session.sql(query).to_pandas()
    
    def execute_sql_no_return(self, query: str) -> bool:
        """Execute SQL without expecting results."""
        try:
            self.session.sql(query).collect()
            return True
        except Exception as e:
            print(f"SQL execution error: {e}")
            return False
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists using Snowpark."""
        try:
            # Try to get table info
            parts = table.split('.')
            if len(parts) == 3:
                db, schema, tbl = parts
                query = f"""
                SELECT COUNT(*) as cnt FROM {db}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{tbl}'
                """
            else:
                query = f"SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}'"
            
            result = self.session.sql(query).to_pandas()
            return result['CNT'].iloc[0] > 0
        except:
            return False
    
    def get_table_columns(self, table: str) -> List[str]:
        """Get column names using Snowpark."""
        try:
            # Use DESCRIBE TABLE
            result = self.session.sql(f"DESCRIBE TABLE {table}").to_pandas()
            return result['name'].tolist()
        except Exception as e:
            print(f"Error getting columns for {table}: {e}")
            return []
    
    def read_raw_llm_results(
        self, 
        table: str, 
        date: str, 
        department: str,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Read LLM results from raw data table."""
        query = f"""
        SELECT 
            CONVERSATION_ID,
            LLM_RESPONSE,
            DATE,
            DEPARTMENT,
            TOKENS_BREAKDOWN
        FROM {table}
        WHERE DATE = '{date}' AND DEPARTMENT = '{department}'
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        return self.session.sql(query).to_pandas()
    
    def upsert_summary_row(
        self, 
        table: str, 
        date: str, 
        department: str, 
        metrics: Dict[str, Any]
    ) -> bool:
        """Insert or update summary row using Snowpark."""
        try:
            # First try to delete existing row
            delete_query = f"""
            DELETE FROM {table}
            WHERE DATE = '{date}' AND DEPARTMENT = '{department}'
            """
            self.session.sql(delete_query).collect()
            
            # Build column list and values
            columns = ['DATE', 'DEPARTMENT']
            values = [f"'{date}'", f"'{department}'"]
            
            for col, val in metrics.items():
                columns.append(col)
                if val is None:
                    values.append('NULL')
                elif isinstance(val, str):
                    # Escape single quotes
                    escaped = val.replace("'", "''")
                    values.append(f"'{escaped}'")
                else:
                    values.append(str(val))
            
            insert_query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(values)})
            """
            
            self.session.sql(insert_query).collect()
            return True
            
        except Exception as e:
            print(f"Error upserting summary row: {e}")
            return False
    
    def close(self) -> None:
        """Snowpark session is managed externally, no-op."""
        pass
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean DataFrame for Snowflake/PyArrow compatibility.
        
        Handles:
        - None/NaN values
        - Object types with mixed content
        - Datetime conversions
        """
        df_clean = df.copy()
        
        for col in df_clean.columns:
            # Handle object columns
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].apply(
                    lambda x: str(x) if x is not None and pd.notna(x) else None
                )
            
            # Handle datetime columns
            if 'datetime' in str(df_clean[col].dtype):
                df_clean[col] = df_clean[col].astype(str)
        
        return df_clean
