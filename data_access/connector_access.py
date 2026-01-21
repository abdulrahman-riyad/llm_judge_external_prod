"""
Snowflake Connector Data Access Implementation

Uses snowflake-connector-python for data operations.
This is used when running externally (GitHub Actions, Colab, local).

Lessons Learned from Colab Deployment (incorporated):
1. Use parameterized queries to handle special characters safely
2. Handle connection lifecycle properly
3. Pandas warning about DBAPI2 is expected and harmless
4. Use fetch_pandas_all() for efficient data retrieval
"""

from typing import Dict, List, Any, Optional
import pandas as pd
import json

from data_access.interface import DataAccessInterface, DataAccessConfig

# snowflake-connector-python import
try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    CONNECTOR_AVAILABLE = True
except ImportError:
    CONNECTOR_AVAILABLE = False
    snowflake = None


class ConnectorDataAccess(DataAccessInterface):
    """
    Data access implementation using snowflake-connector-python.
    
    This is the primary implementation when running externally
    in GitHub Actions, Google Colab, or local development.
    """
    
    def __init__(self, config: DataAccessConfig):
        """
        Initialize with connection configuration.
        
        Args:
            config: DataAccessConfig with account, user, password, etc.
        """
        if not CONNECTOR_AVAILABLE:
            raise RuntimeError(
                "snowflake-connector-python not available. "
                "Install with: pip install snowflake-connector-python"
            )
        
        if not config.is_connector_mode():
            raise ValueError(
                "Invalid config for connector mode. "
                "Required: account, user, password"
            )
        
        self.config = config
        self._connection = None
        self._connect()
    
    def _connect(self) -> None:
        """Establish connection to Snowflake."""
        self._connection = snowflake.connector.connect(
            account=self.config.account,
            user=self.config.user,
            password=self.config.password,
            warehouse=self.config.warehouse,
            database=self.config.database,
            schema=self.config.schema,
            role=self.config.role
        )
    
    def _get_cursor(self):
        """Get a cursor, reconnecting if necessary."""
        if self._connection is None or self._connection.is_closed():
            self._connect()
        return self._connection.cursor()
    
    def read_conversations(
        self, 
        table: str, 
        date: str, 
        department: str,
        skill_filter: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Read conversations using connector."""
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
        WHERE TO_DATE(START_DATE) = %s
        """
        params = [date]
        
        if skill_filter:
            query += f"\n          AND SKILL ILIKE %s"
            params.append(f'%{skill_filter}%')
        
        query += "\n        ORDER BY CONVERSATION_ID, MESSAGE_SENT_TIME"
        
        if limit:
            query += f"\n        LIMIT {limit}"  # LIMIT is safe, not parameterized
        
        cursor = self._get_cursor()
        cursor.execute(query, params)
        df = cursor.fetch_pandas_all()
        cursor.close()
        
        return df
    
    def write_results(
        self, 
        df: pd.DataFrame, 
        table: str,
        mode: str = 'append'
    ) -> int:
        """
        Write results using connector.
        
        Note: For 'overwrite' mode, we truncate first then insert.
        """
        if df.empty:
            return 0
        
        # Parse table name for write_pandas
        parts = table.split('.')
        if len(parts) == 3:
            db, schema, tbl = parts
        elif len(parts) == 2:
            db = self.config.database
            schema, tbl = parts
        else:
            db = self.config.database
            schema = self.config.schema
            tbl = table
        
        # Handle overwrite mode
        if mode == 'overwrite':
            self.execute_sql_no_return(f"TRUNCATE TABLE IF EXISTS {table}")
        
        # Write using write_pandas
        success, nchunks, nrows, _ = write_pandas(
            conn=self._connection,
            df=df,
            table_name=tbl,
            database=db,
            schema=schema,
            auto_create_table=False  # Tables should exist
        )
        
        return nrows if success else 0
    
    def write_results_row_by_row(
        self, 
        df: pd.DataFrame, 
        table: str,
        columns: List[str]
    ) -> tuple:
        """
        Write results row by row with parameterized queries.
        
        This is safer for data with special characters but slower.
        Use for LLM responses which may contain quotes, etc.
        
        Args:
            df: DataFrame to write
            table: Target table name
            columns: List of column names to insert
        
        Returns:
            Tuple of (rows_written, errors)
        """
        cursor = self._get_cursor()
        written = 0
        errors = 0
        
        # Build parameterized INSERT
        placeholders = ', '.join(['%s'] * len(columns))
        insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        for _, row in df.iterrows():
            try:
                values = [row.get(col) for col in columns]
                # Convert None-like values
                values = [
                    None if pd.isna(v) else (str(v) if not isinstance(v, (int, float, bool)) else v)
                    for v in values
                ]
                cursor.execute(insert_sql, values)
                written += 1
            except Exception as e:
                errors += 1
                if errors <= 3:  # Only print first few errors
                    print(f"Error inserting row: {str(e)[:100]}")
        
        self._connection.commit()
        cursor.close()
        
        return written, errors
    
    def execute_sql(self, query: str) -> pd.DataFrame:
        """Execute SQL using connector."""
        cursor = self._get_cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        cursor.close()
        return df
    
    def execute_sql_no_return(self, query: str) -> bool:
        """Execute SQL without expecting results."""
        cursor = self._get_cursor()
        try:
            cursor.execute(query)
            self._connection.commit()
            return True
        except Exception as e:
            print(f"SQL execution error: {e}")
            return False
        finally:
            cursor.close()
    
    def execute_sql_parameterized(self, query: str, params: tuple) -> bool:
        """
        Execute parameterized SQL.
        
        IMPORTANT: Always use this for data containing user content or LLM responses
        to prevent SQL injection and handle special characters.
        
        Args:
            query: SQL with %s placeholders
            params: Tuple of parameter values
        
        Returns:
            True if successful
        """
        cursor = self._get_cursor()
        try:
            cursor.execute(query, params)
            self._connection.commit()
            return True
        except Exception as e:
            print(f"Parameterized SQL error: {e}")
            return False
        finally:
            cursor.close()
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists using connector."""
        try:
            parts = table.split('.')
            if len(parts) == 3:
                db, schema, tbl = parts
                query = f"""
                SELECT COUNT(*) as cnt FROM {db}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """
                cursor = self._get_cursor()
                cursor.execute(query, (schema, tbl))
            else:
                query = "SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = %s"
                cursor = self._get_cursor()
                cursor.execute(query, (table,))
            
            result = cursor.fetchone()
            cursor.close()
            return result[0] > 0
        except:
            return False
    
    def get_table_columns(self, table: str) -> List[str]:
        """Get column names using connector."""
        try:
            cursor = self._get_cursor()
            cursor.execute(f"DESCRIBE TABLE {table}")
            rows = cursor.fetchall()
            cursor.close()
            return [row[0] for row in rows]
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
        WHERE DATE = %s AND DEPARTMENT = %s
        """
        params = [date, department]
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor = self._get_cursor()
        cursor.execute(query, params)
        df = cursor.fetch_pandas_all()
        cursor.close()
        
        return df
    
    def upsert_summary_row(
        self, 
        table: str, 
        date: str, 
        department: str, 
        metrics: Dict[str, Any]
    ) -> bool:
        """Insert or update summary row using connector."""
        try:
            # First delete existing row (use parameterized query)
            delete_sql = f"DELETE FROM {table} WHERE DATE = %s AND DEPARTMENT = %s"
            cursor = self._get_cursor()
            cursor.execute(delete_sql, (date, department))
            self._connection.commit()
            cursor.close()
            
            # Build column list and values for INSERT
            columns = ['DATE', 'DEPARTMENT']
            values = [date, department]
            
            for col, val in metrics.items():
                columns.append(col)
                values.append(val)
            
            # Create parameterized INSERT
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
            
            cursor = self._get_cursor()
            cursor.execute(insert_sql, tuple(values))
            self._connection.commit()
            cursor.close()
            
            return True
            
        except Exception as e:
            print(f"Error upserting summary row: {e}")
            return False
    
    def insert_llm_result(
        self,
        table: str,
        conversation_id: str,
        date: str,
        department: str,
        llm_response: str,
        prompt_type: str = None,
        tokens_breakdown: Optional[Dict] = None
    ) -> bool:
        """
        Insert a single LLM result with proper escaping.
        
        This uses parameterized queries to safely handle special characters
        in LLM responses.
        
        Args:
            table: Target raw data table
            conversation_id: Conversation ID
            date: Target date
            department: Department name
            llm_response: The LLM response text (may contain special chars)
            prompt_type: The prompt type (e.g., 'client_suspecting_ai', 'ftr')
            tokens_breakdown: Optional token usage dict
        
        Returns:
            True if successful
        """
        try:
            # Try with TOKENS_BREAKDOWN as JSON
            if tokens_breakdown:
                insert_sql = f"""
                INSERT INTO {table} 
                (CONVERSATION_ID, DATE, DEPARTMENT, LLM_RESPONSE, PROMPT_TYPE, PROCESSING_STATUS, TOKENS_BREAKDOWN)
                VALUES (%s, %s, %s, %s, %s, 'COMPLETED', PARSE_JSON(%s))
                """
                tokens_json = json.dumps(tokens_breakdown)
                params = (conversation_id, date, department, llm_response, prompt_type, tokens_json)
            else:
                insert_sql = f"""
                INSERT INTO {table} 
                (CONVERSATION_ID, DATE, DEPARTMENT, LLM_RESPONSE, PROMPT_TYPE, PROCESSING_STATUS)
                VALUES (%s, %s, %s, %s, %s, 'COMPLETED')
                """
                params = (conversation_id, date, department, llm_response, prompt_type)
            
            return self.execute_sql_parameterized(insert_sql, params)
            
        except Exception as e:
            # Fallback: Try without TOKENS_BREAKDOWN
            try:
                insert_sql = f"""
                INSERT INTO {table} 
                (CONVERSATION_ID, DATE, DEPARTMENT, LLM_RESPONSE, PROMPT_TYPE, PROCESSING_STATUS)
                VALUES (%s, %s, %s, %s, %s, 'COMPLETED')
                """
                params = (conversation_id, date, department, llm_response, prompt_type)
                return self.execute_sql_parameterized(insert_sql, params)
            except Exception as e2:
                print(f"Error inserting LLM result: {e2}")
                return False
    
    def insert_tool_eval_result(
        self,
        table: str,
        conversation_id: str,
        message_id: str,
        date: str,
        department: str,
        message: str,
        client_attributes: str,
        history: str,
        actual_tools_called: str,
        llm_response: str,
        execution_id: str,
        target_skill: Optional[str] = None,
        customer_name: Optional[str] = None,
        tokens_breakdown: Optional[Dict] = None
    ) -> bool:
        """
        Insert a tool_eval result to TOOL_EVAL_RAW_DATA.
        
        Handles the special schema for message-level tool evaluation with
        MESSAGE_ID, CLIENT_ATTRIBUTES, HISTORY, ACTUAL_TOOLS_CALLED columns.
        
        Args:
            table: Target table (TOOL_EVAL_RAW_DATA)
            conversation_id: Conversation ID
            message_id: Message ID (unique per consumer message)
            date: Target date
            department: Department name
            message: Consumer message text
            client_attributes: JSON string of client attributes from N8N
            history: JSON string of chat history from N8N
            actual_tools_called: JSON array of actual tools called
            llm_response: LLM verdict array as JSON string
            execution_id: N8N execution ID
            target_skill: Target skill for the message
            customer_name: Customer name
            tokens_breakdown: Token usage dict
            
        Returns:
            True if successful
        """
        try:
            insert_sql = f"""
            INSERT INTO {table} (
                CONVERSATION_ID, MESSAGE_ID, DATE, DEPARTMENT,
                MESSAGE, CLIENT_ATTRIBUTES, HISTORY, ACTUAL_TOOLS_CALLED,
                LLM_RESPONSE, EXECUTION_ID, TARGET_SKILL, CUSTOMER_NAME,
                PROCESSING_STATUS, PROMPT_TYPE, IS_PARSED, TIMESTAMP
            )
            VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                'COMPLETED', 'tool_eval', FALSE, CURRENT_TIMESTAMP()
            )
            """
            
            params = (
                conversation_id, message_id, date, department,
                message, client_attributes, history, actual_tools_called,
                llm_response, execution_id, target_skill or '', customer_name or ''
            )
            
            return self.execute_sql_parameterized(insert_sql, params)
            
        except Exception as e:
            # Try simpler insert without optional fields
            try:
                insert_sql = f"""
                INSERT INTO {table} (
                    CONVERSATION_ID, MESSAGE_ID, DATE, DEPARTMENT,
                    MESSAGE, CLIENT_ATTRIBUTES, HISTORY, ACTUAL_TOOLS_CALLED,
                    LLM_RESPONSE, EXECUTION_ID, PROCESSING_STATUS, PROMPT_TYPE
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, 'COMPLETED', 'tool_eval'
                )
                """
                
                params = (
                    conversation_id, message_id, date, department,
                    message, client_attributes, history, actual_tools_called,
                    llm_response, execution_id
                )
                
                return self.execute_sql_parameterized(insert_sql, params)
                
            except Exception as e2:
                print(f"Error inserting tool_eval result: {e2}")
                return False
    
    def close(self) -> None:
        """Close the connection."""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
            self._connection = None
