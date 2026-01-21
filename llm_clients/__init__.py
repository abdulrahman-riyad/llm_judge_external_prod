# =============================================================================
# LLM CLIENTS MODULE
# =============================================================================
# 
# This module provides an abstraction layer for LLM API calls, supporting both:
# - Snowflake UDF execution (existing behavior)
# - External HTTP execution (GitHub Actions / Colab - cost optimized)
#
# =============================================================================

from .interface import (
    LLMClientInterface,
    LLMResponse,
    LLMError,
    APIType,
    ExecutionMode
)

from .factory import (
    create_llm_client,
    detect_execution_environment,
    get_default_client
)

from .n8n_client import (
    N8NClient,
    N8NData,
    set_n8n_client,
    get_n8n_client,
    is_n8n_client_available
)

__all__ = [
    # Interface
    'LLMClientInterface',
    'LLMResponse', 
    'LLMError',
    'APIType',
    'ExecutionMode',
    # Factory
    'create_llm_client',
    'detect_execution_environment',
    'get_default_client',
    # N8N Client
    'N8NClient',
    'N8NData',
    'set_n8n_client',
    'get_n8n_client',
    'is_n8n_client_available',
]
