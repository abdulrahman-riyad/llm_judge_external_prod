"""
Environment Detection and Configuration

Detects the current execution environment and provides appropriate
configuration and setup utilities.

Supported Environments:
- SNOWFLAKE: Running inside Snowflake (worksheets, stored procedures)
- GITHUB_ACTIONS: Running in GitHub Actions CI/CD
- COLAB: Running in Google Colab
- LOCAL: Local development machine
"""

import os
import sys
from enum import Enum
from typing import Optional, Tuple, Any
from dataclasses import dataclass


class Environment(Enum):
    """Execution environment types."""
    SNOWFLAKE = "snowflake"           # Inside Snowflake (Snowpark available)
    GITHUB_ACTIONS = "github_actions"  # GitHub Actions runner
    COLAB = "colab"                    # Google Colab notebook
    LOCAL = "local"                    # Local development


@dataclass
class EnvironmentInfo:
    """Information about the detected environment."""
    environment: Environment
    has_snowpark: bool = False
    has_connector: bool = False
    colab_version: Optional[str] = None
    github_run_id: Optional[str] = None
    
    @property
    def is_external(self) -> bool:
        """Check if running externally (not inside Snowflake)."""
        return self.environment != Environment.SNOWFLAKE
    
    @property
    def can_run_async(self) -> bool:
        """Check if environment supports async/parallel operations."""
        return self.is_external


def detect_environment() -> Environment:
    """
    Auto-detect the current execution environment.
    
    Detection Priority:
    1. GitHub Actions (check GITHUB_ACTIONS env var)
    2. Google Colab (check for colab module in sys.modules)
    3. Snowflake (check if Snowpark is available and functional)
    4. Local (default fallback)
    
    Returns:
        Environment enum value
    """
    # 1. Check GitHub Actions
    if os.getenv('GITHUB_ACTIONS') == 'true':
        return Environment.GITHUB_ACTIONS
    
    # 2. Check Google Colab
    if 'google.colab' in sys.modules:
        return Environment.COLAB
    
    # 3. Check for Colab by checking for /content directory
    if os.path.exists('/content') and 'COLAB' in os.environ.get('SHELL', ''):
        return Environment.COLAB
    
    # 4. Check Snowflake (Snowpark available and functional)
    try:
        import snowflake.snowpark as snowpark
        # Additional check: are we actually inside Snowflake?
        # When running locally with Snowpark installed, we're still "external"
        # Inside Snowflake, certain env vars or behaviors differ
        
        # Check for Snowflake-specific environment indicators
        # When running inside Snowflake, _snowflake module is available
        if '_snowflake' in sys.modules or hasattr(snowpark, '_context'):
            return Environment.SNOWFLAKE
    except ImportError:
        pass
    
    # 5. Default to Local
    return Environment.LOCAL


def get_environment_info() -> EnvironmentInfo:
    """
    Get detailed information about the current environment.
    
    Returns:
        EnvironmentInfo with all detected properties
    """
    env = detect_environment()
    
    # Check for Snowpark availability
    has_snowpark = False
    try:
        import snowflake.snowpark
        has_snowpark = True
    except ImportError:
        pass
    
    # Check for connector availability
    has_connector = False
    try:
        import snowflake.connector
        has_connector = True
    except ImportError:
        pass
    
    # Get environment-specific info
    colab_version = None
    github_run_id = None
    
    if env == Environment.COLAB:
        try:
            import google.colab
            colab_version = getattr(google.colab, '__version__', 'unknown')
        except:
            colab_version = 'unknown'
    
    elif env == Environment.GITHUB_ACTIONS:
        github_run_id = os.getenv('GITHUB_RUN_ID', 'unknown')
    
    return EnvironmentInfo(
        environment=env,
        has_snowpark=has_snowpark,
        has_connector=has_connector,
        colab_version=colab_version,
        github_run_id=github_run_id
    )


def setup_environment(env: Optional[Environment] = None) -> Tuple[Any, Any]:
    """
    Set up the environment for LLM pipeline execution.
    
    This function:
    1. Detects or validates the environment
    2. Sets up necessary imports and configurations
    3. Returns appropriate LLM client and data access objects
    
    Args:
        env: Optional environment to use (auto-detect if None)
    
    Returns:
        Tuple of (LLMClientInterface, DataAccessInterface)
    
    Usage:
        # Auto-detect and setup
        llm_client, data_access = setup_environment()
        
        # Explicit environment
        llm_client, data_access = setup_environment(Environment.COLAB)
    """
    if env is None:
        env = detect_environment()
    
    print(f"🔧 Setting up environment: {env.value}")
    
    if env == Environment.SNOWFLAKE:
        # Inside Snowflake - use Snowpark
        return _setup_snowflake_environment()
    
    elif env == Environment.COLAB:
        # Google Colab - handle module shadowing, use async HTTP
        return _setup_colab_environment()
    
    elif env == Environment.GITHUB_ACTIONS:
        # GitHub Actions - use async HTTP with env var config
        return _setup_github_actions_environment()
    
    else:
        # Local development
        return _setup_local_environment()


def _setup_snowflake_environment() -> Tuple[Any, Any]:
    """Setup for running inside Snowflake."""
    # Import Snowpark-specific implementations
    from llm_clients.snowflake_udf_client import SnowflakeUDFClient
    from data_access.snowpark_access import SnowparkDataAccess
    
    # Note: Session should be passed externally in Snowflake
    # Return factories that need session
    print("   ℹ️  Snowflake environment - session must be provided")
    
    return SnowflakeUDFClient, SnowparkDataAccess


def _setup_colab_environment() -> Tuple[Any, Any]:
    """
    Setup for Google Colab.
    
    Handles common Colab issues:
    - Module shadowing (local snowflake/ folder)
    - Missing packages (auto-suggest pip install)
    - Async event loop (nest_asyncio)
    """
    import shutil
    
    # Clean up module shadowing
    # Lesson learned: local snowflake/ folder can shadow installed package
    if os.path.exists('snowflake'):
        print("   🧹 Removing local snowflake/ folder (prevents module shadowing)")
        shutil.rmtree('snowflake')
    
    # Clear cached modules
    modules_to_clear = [k for k in sys.modules.keys() if k.startswith('snowflake')]
    for mod in modules_to_clear:
        del sys.modules[mod]
    if modules_to_clear:
        print(f"   🧹 Cleared {len(modules_to_clear)} cached snowflake modules")
    
    # Apply nest_asyncio for event loop
    try:
        import nest_asyncio
        nest_asyncio.apply()
        print("   ✅ Applied nest_asyncio for async support")
    except ImportError:
        print("   ⚠️  nest_asyncio not available - install with: pip install nest_asyncio")
    
    # Import async client and connector-based data access
    from llm_clients.async_http_client import AsyncHTTPClient
    from data_access.connector_access import ConnectorDataAccess
    
    print("   ✅ Colab environment ready")
    
    return AsyncHTTPClient, ConnectorDataAccess


def _setup_github_actions_environment() -> Tuple[Any, Any]:
    """Setup for GitHub Actions."""
    # Similar to Colab but without interactive concerns
    from llm_clients.async_http_client import AsyncHTTPClient
    from data_access.connector_access import ConnectorDataAccess
    
    print("   ✅ GitHub Actions environment ready")
    print(f"   📋 Run ID: {os.getenv('GITHUB_RUN_ID', 'unknown')}")
    
    return AsyncHTTPClient, ConnectorDataAccess


def _setup_local_environment() -> Tuple[Any, Any]:
    """Setup for local development."""
    # Try to use best available option
    try:
        from llm_clients.async_http_client import AsyncHTTPClient
        from data_access.connector_access import ConnectorDataAccess
        print("   ✅ Local environment ready (using connector + async HTTP)")
        return AsyncHTTPClient, ConnectorDataAccess
    except ImportError as e:
        print(f"   ⚠️  Import error: {e}")
        print("   💡 Make sure all dependencies are installed")
        raise


def print_environment_summary():
    """Print a summary of the detected environment."""
    info = get_environment_info()
    
    print("=" * 60)
    print("📋 ENVIRONMENT SUMMARY")
    print("=" * 60)
    print(f"   Environment: {info.environment.value}")
    print(f"   Snowpark Available: {'✅' if info.has_snowpark else '❌'}")
    print(f"   Connector Available: {'✅' if info.has_connector else '❌'}")
    print(f"   External Execution: {'✅' if info.is_external else '❌'}")
    print(f"   Async Support: {'✅' if info.can_run_async else '❌'}")
    
    if info.colab_version:
        print(f"   Colab Version: {info.colab_version}")
    if info.github_run_id:
        print(f"   GitHub Run ID: {info.github_run_id}")
    
    print("=" * 60)
