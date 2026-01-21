# =============================================================================
# LLM CLIENT FACTORY
# =============================================================================
#
# Factory pattern for creating LLM clients based on execution environment.
#
# LESSON LEARNED: Environment detection must be robust to avoid import errors.
# Module shadowing (local snowflake/ folder) can break real imports.
#
# =============================================================================

import os
import sys
from typing import Dict, Any, Optional

from .interface import LLMClientInterface, ExecutionMode


# =============================================================================
# ENVIRONMENT DETECTION
# =============================================================================

def detect_execution_environment() -> ExecutionMode:
    """
    Auto-detect the current execution environment.
    
    LESSON: Must check for Snowflake BEFORE importing snowpark,
    as snowpark may not be available in external environments.
    
    Returns:
        ExecutionMode indicating where the code is running
    """
    # Check for GitHub Actions
    if os.getenv('GITHUB_ACTIONS'):
        return ExecutionMode.EXTERNAL_HTTP
    
    # Check for Google Colab
    if 'google.colab' in sys.modules:
        return ExecutionMode.EXTERNAL_HTTP
    
    # Check for explicit environment variable
    env_mode = os.getenv('LLM_EXECUTION_MODE', '').lower()
    if env_mode == 'external_http':
        return ExecutionMode.EXTERNAL_HTTP
    elif env_mode == 'snowflake_udf':
        return ExecutionMode.SNOWFLAKE_UDF
    elif env_mode == 'hybrid':
        return ExecutionMode.HYBRID
    
    # Check if running inside Snowflake
    # LESSON: Use _snowflake module check, not snowpark import
    try:
        import _snowflake  # Only available inside Snowflake
        return ExecutionMode.SNOWFLAKE_UDF
    except ImportError:
        pass
    
    # Default to external HTTP (safer - avoids Snowflake billing)
    return ExecutionMode.EXTERNAL_HTTP


# =============================================================================
# CLIENT CREATION
# =============================================================================

# Global client instance (singleton pattern)
_default_client: Optional[LLMClientInterface] = None


def create_llm_client(
    mode: ExecutionMode = None,
    config: Dict[str, Any] = None
) -> LLMClientInterface:
    """
    Create an LLM client for the specified execution mode.
    
    Args:
        mode: Execution mode (auto-detected if None)
        config: Configuration dict with credentials and settings
            For SNOWFLAKE_UDF:
                - session: Snowpark Session object
            For EXTERNAL_HTTP:
                - api_key: OpenAI API key
                - api_keys: Dict of department -> API key (optional)
                - batch_size: Batch size for parallel calls (default 20)
                - delay: Delay between batches in seconds (default 1.0)
                - max_retries: Max retry attempts (default 3)
                - timeout: Request timeout in seconds (default 180)
    
    Returns:
        LLMClientInterface implementation
    
    Raises:
        ValueError: If required config is missing
        ImportError: If required modules are not available
    """
    if mode is None:
        mode = detect_execution_environment()
    
    if config is None:
        config = {}
    
    if mode == ExecutionMode.SNOWFLAKE_UDF:
        return _create_snowflake_client(config)
    elif mode == ExecutionMode.EXTERNAL_HTTP:
        return _create_external_client(config)
    elif mode == ExecutionMode.HYBRID:
        return _create_external_client(config)  # Use external for LLM calls
    else:
        raise ValueError(f"Unknown execution mode: {mode}")


def _create_snowflake_client(config: Dict[str, Any]) -> LLMClientInterface:
    """Create Snowflake UDF client."""
    # Import here to avoid issues when not in Snowflake
    from .snowflake_udf_client import SnowflakeUDFClient
    
    session = config.get('session')
    if session is None:
        raise ValueError("Snowflake session required for SNOWFLAKE_UDF mode")
    
    return SnowflakeUDFClient(session)


def _create_external_client(config: Dict[str, Any]) -> LLMClientInterface:
    """Create external HTTP client."""
    from .async_http_client import AsyncHTTPClient
    
    # Get API key(s)
    api_key = config.get('api_key')
    api_keys = config.get('api_keys', {})
    
    if not api_key and not api_keys:
        # Try environment variables
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError(
                "OpenAI API key required. Provide 'api_key' in config or "
                "set OPENAI_API_KEY environment variable."
            )
    
    return AsyncHTTPClient(
        api_key=api_key or api_keys.get('default'),
        api_keys=api_keys,
        batch_size=config.get('batch_size', 20),
        delay_between_batches=config.get('delay', 1.0),
        max_retries=config.get('max_retries', 3),
        timeout_seconds=config.get('timeout', 180)
    )


def get_default_client() -> LLMClientInterface:
    """
    Get or create the default LLM client.
    
    Uses singleton pattern - creates client once and reuses.
    Client is created based on auto-detected environment.
    
    Returns:
        LLMClientInterface instance
    """
    global _default_client
    
    if _default_client is None:
        _default_client = create_llm_client()
    
    return _default_client


def set_default_client(client: LLMClientInterface) -> None:
    """Set the default LLM client."""
    global _default_client
    _default_client = client


def reset_default_client() -> None:
    """Reset the default client (for testing)."""
    global _default_client
    _default_client = None


# =============================================================================
# CONFIGURATION HELPERS
# =============================================================================

def load_config_from_env() -> Dict[str, Any]:
    """
    Load configuration from environment variables.
    
    Environment Variables:
        OPENAI_API_KEY: Default OpenAI API key
        OPENAI_KEY_<DEPARTMENT>: Department-specific API keys
        LLM_BATCH_SIZE: Batch size for parallel calls
        LLM_DELAY: Delay between batches
        LLM_MAX_RETRIES: Maximum retry attempts
        LLM_TIMEOUT: Request timeout in seconds
        SNOWFLAKE_ACCOUNT: Snowflake account identifier
        SNOWFLAKE_USER: Snowflake username
        SNOWFLAKE_PASSWORD: Snowflake password
        SNOWFLAKE_WAREHOUSE: Snowflake warehouse
        SNOWFLAKE_DATABASE: Snowflake database
        SNOWFLAKE_SCHEMA: Snowflake schema
        SNOWFLAKE_ROLE: Snowflake role
    """
    config = {}
    
    # OpenAI settings
    default_key = os.getenv('OPENAI_API_KEY')
    if default_key:
        config['api_key'] = default_key
    
    # Department-specific keys
    api_keys = {'default': default_key} if default_key else {}
    for key, value in os.environ.items():
        if key.startswith('OPENAI_KEY_'):
            dept = key.replace('OPENAI_KEY_', '')
            api_keys[dept] = value
    if api_keys:
        config['api_keys'] = api_keys
    
    # Batch settings
    if os.getenv('LLM_BATCH_SIZE'):
        config['batch_size'] = int(os.getenv('LLM_BATCH_SIZE'))
    if os.getenv('LLM_DELAY'):
        config['delay'] = float(os.getenv('LLM_DELAY'))
    if os.getenv('LLM_MAX_RETRIES'):
        config['max_retries'] = int(os.getenv('LLM_MAX_RETRIES'))
    if os.getenv('LLM_TIMEOUT'):
        config['timeout'] = int(os.getenv('LLM_TIMEOUT'))
    
    # Snowflake settings (for data access, not UDFs)
    snowflake_config = {}
    for key in ['ACCOUNT', 'USER', 'PASSWORD', 'WAREHOUSE', 'DATABASE', 'SCHEMA', 'ROLE']:
        env_key = f'SNOWFLAKE_{key}'
        if os.getenv(env_key):
            snowflake_config[key.lower()] = os.getenv(env_key)
    if snowflake_config:
        config['snowflake'] = snowflake_config
    
    return config
