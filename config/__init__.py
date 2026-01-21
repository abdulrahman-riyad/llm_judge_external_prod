# =============================================================================
# CONFIGURATION MODULE
# =============================================================================
#
# This module provides configuration management for the LLM_JUDGE pipeline.
# It supports:
# - Environment detection (Snowflake, GitHub Actions, Colab, Local)
# - Configuration loading (environment variables, files)
# - Centralized execution settings
#
# =============================================================================

from .execution_config import (
    ExecutionConfig,
    SnowflakeConfig,
    OpenAIConfig,
    N8NConfig,
    load_config_from_env,
    load_config_from_file,
    get_default_config,
    set_default_config,
)

from .environment import (
    Environment,
    EnvironmentInfo,
    detect_environment,
    get_environment_info,
    setup_environment,
    print_environment_summary,
)

__all__ = [
    # Execution Config
    'ExecutionConfig',
    'SnowflakeConfig',
    'OpenAIConfig',
    'load_config_from_env',
    'load_config_from_file',
    'get_default_config',
    'set_default_config',
    # Environment
    'Environment',
    'EnvironmentInfo',
    'detect_environment',
    'get_environment_info',
    'setup_environment',
    'print_environment_summary',
]
