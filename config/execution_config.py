# =============================================================================
# EXECUTION CONFIGURATION
# =============================================================================
#
# Centralized configuration for LLM pipeline execution.
# Supports loading from environment variables, files, or direct instantiation.
#
# =============================================================================

import os
import json
import yaml
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from pathlib import Path

# Import execution mode from llm_clients
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    from llm_clients.interface import ExecutionMode
except ImportError:
    from enum import Enum
    class ExecutionMode(Enum):
        SNOWFLAKE_UDF = "snowflake_udf"
        EXTERNAL_HTTP = "external_http"
        HYBRID = "hybrid"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration."""
    account: str = ""
    user: str = ""
    password: str = ""
    warehouse: str = "LLMS_WH"
    database: str = "LLM_EVAL"
    schema: str = "PUBLIC"
    role: str = "LLM_ROLE"
    
    def to_dict(self) -> Dict[str, str]:
        """Convert to connection dict."""
        return {
            'account': self.account,
            'user': self.user,
            'password': self.password,
            'warehouse': self.warehouse,
            'database': self.database,
            'schema': self.schema,
            'role': self.role
        }
    
    def is_valid(self) -> bool:
        """Check if config has required fields."""
        return bool(self.account and self.user and self.password)


@dataclass
class OpenAIConfig:
    """OpenAI API configuration."""
    default_key: str = ""
    department_keys: Dict[str, str] = field(default_factory=dict)
    batch_size: int = 20
    delay_between_batches: float = 1.0
    max_retries: int = 3
    timeout_seconds: int = 180
    
    def get_key(self, department: str = None) -> str:
        """Get API key for a department."""
        if department and department in self.department_keys:
            return self.department_keys[department]
        return self.default_key
    
    def is_valid(self) -> bool:
        """Check if config has required fields."""
        return bool(self.default_key or self.department_keys)


@dataclass
class N8NConfig:
    """N8N API configuration for tool_eval message-level processing."""
    api_key: str = ""
    base_url: str = "https://n8n.teljoy.io/api/v1/executions"
    timeout: int = 30
    max_retries: int = 3
    batch_size: int = 20
    delay_between_batches: float = 0.5
    
    def is_valid(self) -> bool:
        """Check if config has required API key."""
        return bool(self.api_key)


@dataclass
class ExecutionConfig:
    """
    Complete configuration for LLM pipeline execution.
    
    Supports multiple execution modes:
    - SNOWFLAKE_UDF: Run LLM calls as Snowflake UDFs (existing behavior)
    - EXTERNAL_HTTP: Run LLM calls from external environment (GitHub Actions/Colab)
    - HYBRID: Data from Snowflake, LLM calls external
    """
    mode: ExecutionMode = ExecutionMode.EXTERNAL_HTTP
    snowflake: SnowflakeConfig = field(default_factory=SnowflakeConfig)
    openai: OpenAIConfig = field(default_factory=OpenAIConfig)
    n8n: N8NConfig = field(default_factory=N8NConfig)
    
    # Pipeline settings
    departments: List[str] = field(default_factory=lambda: ['MV_Resolvers'])
    target_date: Optional[str] = None  # None = yesterday
    prompts_to_run: List[str] = field(default_factory=lambda: ['*'])  # '*' = all
    calculate_metrics: bool = True
    
    # Logging settings
    verbose: bool = True
    log_level: str = "INFO"
    
    def is_valid(self) -> bool:
        """Check if configuration is valid for the selected mode."""
        if self.mode == ExecutionMode.SNOWFLAKE_UDF:
            return self.snowflake.is_valid()
        elif self.mode in (ExecutionMode.EXTERNAL_HTTP, ExecutionMode.HYBRID):
            return self.openai.is_valid() and self.snowflake.is_valid()
        return False
    
    def is_n8n_available(self) -> bool:
        """Check if N8N API is configured (for tool_eval)."""
        return self.n8n.is_valid()


# =============================================================================
# CONFIGURATION LOADERS
# =============================================================================

def load_config_from_env() -> ExecutionConfig:
    """
    Load configuration from environment variables.
    
    Environment Variables:
        LLM_EXECUTION_MODE: 'snowflake_udf', 'external_http', or 'hybrid'
        
        # OpenAI
        OPENAI_API_KEY: Default API key
        OPENAI_KEY_<DEPARTMENT>: Department-specific keys (e.g., OPENAI_KEY_MV_RESOLVERS)
        LLM_BATCH_SIZE: Batch size (default 20)
        LLM_DELAY: Delay between batches (default 1.0)
        LLM_MAX_RETRIES: Max retries (default 3)
        LLM_TIMEOUT: Request timeout (default 180)
        
        # Snowflake
        SNOWFLAKE_ACCOUNT: Account identifier
        SNOWFLAKE_USER: Username
        SNOWFLAKE_PASSWORD: Password
        SNOWFLAKE_WAREHOUSE: Warehouse (default LLMS_WH)
        SNOWFLAKE_DATABASE: Database (default LLM_EVAL)
        SNOWFLAKE_SCHEMA: Schema (default PUBLIC)
        SNOWFLAKE_ROLE: Role (default LLM_ROLE)
        
        # N8N (for tool_eval)
        N8N_API_KEY: N8N API key
        N8N_BASE_URL: N8N API base URL (default https://n8n.teljoy.io/api/v1/executions)
        
        # Pipeline
        LLM_DEPARTMENTS: Comma-separated list of departments
        LLM_TARGET_DATE: Target date (YYYY-MM-DD format)
        LLM_PROMPTS: Comma-separated list of prompts (* for all)
        LLM_CALCULATE_METRICS: true/false
    """
    # Execution mode
    mode_str = os.getenv('LLM_EXECUTION_MODE', 'external_http').lower()
    mode = ExecutionMode(mode_str) if mode_str in [m.value for m in ExecutionMode] else ExecutionMode.EXTERNAL_HTTP
    
    # OpenAI config
    default_key = os.getenv('OPENAI_API_KEY', '')
    department_keys = {}
    for key, value in os.environ.items():
        if key.startswith('OPENAI_KEY_'):
            dept = key.replace('OPENAI_KEY_', '').replace('_', ' ').title().replace(' ', '_')
            department_keys[dept] = value
    
    openai_config = OpenAIConfig(
        default_key=default_key,
        department_keys=department_keys,
        batch_size=int(os.getenv('LLM_BATCH_SIZE', '20')),
        delay_between_batches=float(os.getenv('LLM_DELAY', '1.0')),
        max_retries=int(os.getenv('LLM_MAX_RETRIES', '3')),
        timeout_seconds=int(os.getenv('LLM_TIMEOUT', '180'))
    )
    
    # Snowflake config
    snowflake_config = SnowflakeConfig(
        account=os.getenv('SNOWFLAKE_ACCOUNT', ''),
        user=os.getenv('SNOWFLAKE_USER', ''),
        password=os.getenv('SNOWFLAKE_PASSWORD', ''),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'LLMS_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'LLM_EVAL'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
        role=os.getenv('SNOWFLAKE_ROLE', 'LLM_ROLE')
    )
    
    # N8N config (for tool_eval message-level processing)
    n8n_config = N8NConfig(
        api_key=os.getenv('N8N_API_KEY', ''),
        base_url=os.getenv('N8N_BASE_URL', 'https://n8n.teljoy.io/api/v1/executions')
    )
    
    # Pipeline settings
    departments_str = os.getenv('LLM_DEPARTMENTS', 'MV_Resolvers')
    departments = [d.strip() for d in departments_str.split(',')]
    
    prompts_str = os.getenv('LLM_PROMPTS', '*')
    prompts = [p.strip() for p in prompts_str.split(',')] if prompts_str != '*' else ['*']
    
    return ExecutionConfig(
        mode=mode,
        snowflake=snowflake_config,
        openai=openai_config,
        n8n=n8n_config,
        departments=departments,
        target_date=os.getenv('LLM_TARGET_DATE'),
        prompts_to_run=prompts,
        calculate_metrics=os.getenv('LLM_CALCULATE_METRICS', 'true').lower() == 'true',
        verbose=os.getenv('LLM_VERBOSE', 'true').lower() == 'true',
        log_level=os.getenv('LLM_LOG_LEVEL', 'INFO')
    )


def load_config_from_file(path: str) -> ExecutionConfig:
    """
    Load configuration from a JSON or YAML file.
    
    Args:
        path: Path to config file (.json, .yaml, or .yml)
    
    Returns:
        ExecutionConfig instance
    """
    path = Path(path)
    
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    
    with open(path) as f:
        if path.suffix in ('.yaml', '.yml'):
            data = yaml.safe_load(f)
        else:
            data = json.load(f)
    
    # Parse mode
    mode_str = data.get('mode', 'external_http').lower()
    mode = ExecutionMode(mode_str) if mode_str in [m.value for m in ExecutionMode] else ExecutionMode.EXTERNAL_HTTP
    
    # Parse OpenAI config
    openai_data = data.get('openai', {})
    openai_config = OpenAIConfig(
        default_key=openai_data.get('default_key', ''),
        department_keys=openai_data.get('department_keys', {}),
        batch_size=openai_data.get('batch_size', 20),
        delay_between_batches=openai_data.get('delay', 1.0),
        max_retries=openai_data.get('max_retries', 3),
        timeout_seconds=openai_data.get('timeout', 180)
    )
    
    # Parse Snowflake config
    sf_data = data.get('snowflake', {})
    snowflake_config = SnowflakeConfig(
        account=sf_data.get('account', ''),
        user=sf_data.get('user', ''),
        password=sf_data.get('password', ''),
        warehouse=sf_data.get('warehouse', 'LLMS_WH'),
        database=sf_data.get('database', 'LLM_EVAL'),
        schema=sf_data.get('schema', 'PUBLIC'),
        role=sf_data.get('role', 'LLM_ROLE')
    )
    
    # Parse pipeline settings
    pipeline_data = data.get('pipeline', {})
    
    return ExecutionConfig(
        mode=mode,
        snowflake=snowflake_config,
        openai=openai_config,
        departments=pipeline_data.get('departments', ['MV_Resolvers']),
        target_date=pipeline_data.get('target_date'),
        prompts_to_run=pipeline_data.get('prompts', ['*']),
        calculate_metrics=pipeline_data.get('calculate_metrics', True),
        verbose=data.get('verbose', True),
        log_level=data.get('log_level', 'INFO')
    )


# Global config instance
_default_config: Optional[ExecutionConfig] = None


def get_default_config() -> ExecutionConfig:
    """Get or create default configuration from environment."""
    global _default_config
    if _default_config is None:
        _default_config = load_config_from_env()
    return _default_config


def set_default_config(config: ExecutionConfig) -> None:
    """Set the default configuration."""
    global _default_config
    _default_config = config


# =============================================================================
# CONFIG FILE TEMPLATE
# =============================================================================

CONFIG_TEMPLATE = """
# LLM Pipeline Configuration
# ==========================

mode: external_http  # snowflake_udf, external_http, or hybrid

openai:
  default_key: ""  # Or use environment variable OPENAI_API_KEY
  department_keys:
    MV_Resolvers: ""
    CC_Resolvers: ""
  batch_size: 20
  delay: 1.0
  max_retries: 3
  timeout: 180

snowflake:
  account: ""
  user: ""
  password: ""
  warehouse: LLMS_WH
  database: LLM_EVAL
  schema: PUBLIC
  role: LLM_ROLE

pipeline:
  departments:
    - MV_Resolvers
    - CC_Resolvers
  target_date: null  # null = yesterday
  prompts:
    - "*"  # * = all prompts
  calculate_metrics: true

verbose: true
log_level: INFO
"""


def create_config_template(path: str = "llm_config.yaml") -> None:
    """Create a config file template."""
    with open(path, 'w') as f:
        f.write(CONFIG_TEMPLATE)
    print(f"✅ Config template created: {path}")
