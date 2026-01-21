"""
Data Access Interface Definition

Provides abstract base class for data access operations, enabling
the LLM_JUDGE pipeline to work with different database access methods.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Union
import pandas as pd


@dataclass
class TableConfig:
    """Configuration for a database table."""
    database: str = "LLM_EVAL"
    schema: str = "PUBLIC"
    table_name: str = ""
    
    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        if self.database and self.schema:
            return f"{self.database}.{self.schema}.{self.table_name}"
        elif self.schema:
            return f"{self.schema}.{self.table_name}"
        return self.table_name
    
    @classmethod
    def from_string(cls, full_name: str) -> 'TableConfig':
        """Create from full table name string like 'DATABASE.SCHEMA.TABLE'."""
        parts = full_name.split('.')
        if len(parts) == 3:
            return cls(database=parts[0], schema=parts[1], table_name=parts[2])
        elif len(parts) == 2:
            return cls(database="", schema=parts[0], table_name=parts[1])
        return cls(database="", schema="", table_name=full_name)


@dataclass
class DataAccessConfig:
    """
    Configuration for data access layer.
    
    Supports both Snowpark and Connector modes with appropriate settings.
    """
    # Common settings
    database: str = "LLM_EVAL"
    schema: str = "PUBLIC"
    warehouse: str = "LLMS_WH"
    role: str = "LLM_ROLE"
    
    # Connector-specific settings (for external execution)
    account: str = ""
    user: str = ""
    password: str = ""
    
    # Snowpark-specific (session passed directly)
    snowpark_session: Any = None
    
    def is_snowpark_mode(self) -> bool:
        """Check if running in Snowpark mode."""
        return self.snowpark_session is not None
    
    def is_connector_mode(self) -> bool:
        """Check if credentials are available for connector mode."""
        return bool(self.account and self.user and self.password)


class DataAccessInterface(ABC):
    """
    Abstract interface for database operations.
    
    Implementations:
    - SnowparkDataAccess: Uses Snowpark session (inside Snowflake)
    - ConnectorDataAccess: Uses snowflake-connector-python (external)
    """
    
    @abstractmethod
    def read_conversations(
        self, 
        table: str, 
        date: str, 
        department: str,
        skill_filter: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Read conversations from source table.
        
        Args:
            table: Full table name (e.g., 'SILVER.CHAT_EVALS.CC_CLIENT_CHATS')
            date: Target date string (YYYY-MM-DD)
            department: Department name for filtering
            skill_filter: Optional skill/bot filter
            limit: Optional limit on number of conversations
        
        Returns:
            DataFrame with conversation messages
        """
        pass
    
    @abstractmethod
    def write_results(
        self, 
        df: pd.DataFrame, 
        table: str,
        mode: str = 'append'
    ) -> int:
        """
        Write results to output table.
        
        Args:
            df: DataFrame with results to write
            table: Full table name
            mode: 'append' or 'overwrite'
        
        Returns:
            Number of rows written
        """
        pass
    
    @abstractmethod
    def execute_sql(self, query: str) -> pd.DataFrame:
        """
        Execute arbitrary SQL query.
        
        Args:
            query: SQL query string
        
        Returns:
            DataFrame with query results
        """
        pass
    
    @abstractmethod
    def execute_sql_no_return(self, query: str) -> bool:
        """
        Execute SQL that doesn't return results (INSERT, UPDATE, DELETE).
        
        Args:
            query: SQL statement
        
        Returns:
            True if successful
        """
        pass
    
    @abstractmethod
    def table_exists(self, table: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            table: Full table name
        
        Returns:
            True if table exists
        """
        pass
    
    @abstractmethod
    def get_table_columns(self, table: str) -> List[str]:
        """
        Get list of column names for a table.
        
        Args:
            table: Full table name
        
        Returns:
            List of column names
        """
        pass
    
    @abstractmethod
    def read_raw_llm_results(
        self, 
        table: str, 
        date: str, 
        department: str,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Read LLM results from raw data table.
        
        Args:
            table: Raw data table name
            date: Target date
            department: Department name
            limit: Optional limit
        
        Returns:
            DataFrame with LLM responses
        """
        pass
    
    @abstractmethod
    def upsert_summary_row(
        self, 
        table: str, 
        date: str, 
        department: str, 
        metrics: Dict[str, Any]
    ) -> bool:
        """
        Insert or update a summary table row.
        
        Args:
            table: Summary table name
            date: Target date
            department: Department name
            metrics: Dictionary of metric column names and values
        
        Returns:
            True if successful
        """
        pass
    
    @abstractmethod
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
        tokens_breakdown: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Insert a tool_eval result to TOOL_EVAL_RAW_DATA.
        
        This handles the special schema for message-level tool evaluation.
        
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
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close any open connections."""
        pass
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connection cleanup."""
        self.close()
        return False
