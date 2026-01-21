"""
Pytest configuration and shared fixtures for LLM_JUDGE test suite.
"""
import pytest
import pandas as pd
import sys
import os
from unittest.mock import MagicMock, patch
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# =============================================================================
# MOCK SNOWPARK FIXTURE
# =============================================================================

@pytest.fixture(scope="session", autouse=True)
def mock_snowpark():
    """Mock snowflake.snowpark module before any imports."""
    import types
    
    mock_snowpark_module = types.ModuleType('snowflake.snowpark')
    mock_snowpark_module.Session = MagicMock
    mock_snowpark_module.DataFrame = MagicMock
    mock_snowpark_module.Row = MagicMock
    
    # Mock types
    mock_types = types.ModuleType('snowflake.snowpark.types')
    mock_types.StringType = str
    mock_types.IntegerType = int
    mock_types.FloatType = float
    mock_types.BooleanType = bool
    mock_types.DateType = object
    mock_types.TimestampType = object
    mock_types.VariantType = object
    mock_types.ArrayType = list
    mock_snowpark_module.types = mock_types
    
    # Mock functions
    mock_functions = types.ModuleType('snowflake.snowpark.functions')
    mock_functions.col = lambda x: x
    mock_functions.lit = lambda x: x
    mock_functions.when = lambda cond, val: val
    mock_snowpark_module.functions = mock_functions
    
    # Register mocks
    if 'snowflake' not in sys.modules:
        mock_snowflake = types.ModuleType('snowflake')
        sys.modules['snowflake'] = mock_snowflake
    
    sys.modules['snowflake'].snowpark = mock_snowpark_module
    sys.modules['snowflake.snowpark'] = mock_snowpark_module
    sys.modules['snowflake.snowpark.types'] = mock_types
    sys.modules['snowflake.snowpark.functions'] = mock_functions
    
    yield mock_snowpark_module


# =============================================================================
# SAMPLE DATA FIXTURES
# =============================================================================

@pytest.fixture
def sample_messages_df():
    """Sample conversation messages DataFrame with all required columns for XML conversion."""
    return pd.DataFrame({
        'CONVERSATION_ID': ['conv1', 'conv1', 'conv1', 'conv2', 'conv2'],
        'MESSAGE_ID': ['msg1', 'msg2', 'msg3', 'msg4', 'msg5'],
        'SENT_BY': ['consumer', 'bot', 'consumer', 'consumer', 'bot'],  # Use 'consumer' not 'client'
        'TEXT': [
            'Hello, I need help',
            'Hi! How can I assist you today?',
            'I want to replace my maid',
            'My maid is sick',
            'I understand. Let me help you with that.'
        ],
        'MESSAGE_TYPE': ['text', 'text', 'text', 'text', 'text'],
        'MESSAGE_SENT_TIME': pd.to_datetime([
            '2026-01-15 10:00:00',
            '2026-01-15 10:00:05',
            '2026-01-15 10:01:00',
            '2026-01-15 11:00:00',
            '2026-01-15 11:00:10'
        ]),
        'SKILL': ['GPT_MV_RESOLVERS'] * 5,
        'START_DATE': pd.to_datetime(['2026-01-15'] * 5),
        # Additional columns needed for XML conversion
        'TARGET_SKILL_PER_MESSAGE': ['GPT_MV_RESOLVERS'] * 5,
        'SHADOWED_BY': [None, None, None, None, None],
        'AGENT_NAME': [None, None, None, None, None],
        'CUSTOMER_NAME': ['Test Customer'] * 5,
        'CUSTOMER_TYPE': ['returning'] * 5,
    })


@pytest.fixture
def sample_xml_df():
    """Sample XML-formatted conversations DataFrame."""
    return pd.DataFrame({
        'CONVERSATION_ID': ['conv1', 'conv2'],
        'CONVERSATION_XML': [
            '<conversation><message role="client">Hello</message><message role="bot">Hi!</message></conversation>',
            '<conversation><message role="client">Help me</message><message role="bot">Sure!</message></conversation>'
        ],
        'MESSAGE_COUNT': [2, 2]
    })


@pytest.fixture
def sample_llm_response():
    """Sample LLM API response (Chat Completions format)."""
    return {
        'id': 'chatcmpl-123',
        'object': 'chat.completion',
        'created': 1234567890,
        'model': 'gpt-4o-mini',
        'choices': [{
            'index': 0,
            'message': {
                'role': 'assistant',
                'content': '{"ClientSuspectingAI": false}'
            },
            'finish_reason': 'stop'
        }],
        'usage': {
            'prompt_tokens': 100,
            'completion_tokens': 20,
            'total_tokens': 120
        }
    }


@pytest.fixture
def sample_responses_api_response():
    """Sample LLM API response (Responses API format for gpt-5)."""
    return {
        'id': 'resp-123',
        'output_text': '{"ClientSuspectingAI": true}',
        'output': [{
            'type': 'message',
            'content': [{'text': '{"ClientSuspectingAI": true}'}]
        }],
        'usage': {
            'input_tokens': 100,
            'output_tokens': 20,
            'total_tokens': 120
        }
    }


# =============================================================================
# DEPARTMENT CONFIG FIXTURES
# =============================================================================

@pytest.fixture
def sample_department_config():
    """Sample department configuration."""
    return {
        'MV_Resolvers': {
            'bot_skills': ['GPT_MV_RESOLVERS'],
            'agent_skills': ['MV_RESOLVERS_SENIORS'],
            'table_name': 'SILVER.CHAT_EVALS.MV_CLIENTS_CHATS',
            'skill_filter': 'gpt_mv_resolvers',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'MV Resolvers V2'
        },
        'CC_Resolvers': {
            'bot_skills': ['GPT_CC_RESOLVERS'],
            'agent_skills': ['CC_RESOLVERS_AGENTS'],
            'table_name': 'SILVER.CHAT_EVALS.CC_CLIENT_CHATS',
            'skill_filter': 'gpt_cc_resolvers',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'ChatGPT CC Resolvers'
        }
    }


@pytest.fixture
def sample_prompt_config():
    """Sample prompt configuration."""
    return {
        'client_suspecting_ai': {
            'system_prompt': 'Analyze if the client suspects they are talking to AI.',
            'model': 'gpt-5-nano',
            'temperature': 0.2,
            'max_tokens': 1000,
            'output_table': 'CLIENT_SUSPECTING_AI_RAW_DATA',
            'conversion_type': 'xml'
        },
        'false_promises': {
            'system_prompt': 'Identify if false promises were made.',
            'model': 'gpt-5',
            'temperature': 0.1,
            'max_tokens': 2000,
            'output_table': 'FALSE_PROMISES_RAW_DATA',
            'conversion_type': 'xml'
        }
    }


# =============================================================================
# MOCK HTTP FIXTURES
# =============================================================================

@pytest.fixture
def mock_aiohttp_session():
    """Mock aiohttp ClientSession for testing async HTTP calls."""
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.json = MagicMock(return_value={
        'choices': [{
            'message': {'content': '{"result": true}'}
        }],
        'usage': {'prompt_tokens': 10, 'completion_tokens': 5, 'total_tokens': 15}
    })
    mock_session.post.return_value.__aenter__ = MagicMock(return_value=mock_response)
    mock_session.post.return_value.__aexit__ = MagicMock(return_value=None)
    return mock_session
