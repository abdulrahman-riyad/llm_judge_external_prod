# =============================================================================
# LLM CLIENT INTERFACE
# =============================================================================
#
# Abstract interface for LLM API calls with support for multiple execution modes.
#
# LESSONS LEARNED FROM COLAB DEPLOYMENT (incorporated):
# 1. Dual API support: Chat Completions API vs Responses API
# 2. gpt-5 models don't support temperature parameter
# 3. Need robust error handling (check truthy errors, handle dict/string formats)
# 4. Flexible response parsing (output_text, output array with type="message")
# 5. Module shadowing can occur - environment detection must be robust
#
# =============================================================================

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import json
import re


# =============================================================================
# ENUMS
# =============================================================================

class APIType(Enum):
    """
    OpenAI API type to use for a given model.
    
    LESSON: gpt-5 models use Responses API, gpt-4 models use Chat Completions.
    This was discovered during Colab deployment when all gpt-5 calls failed.
    """
    CHAT_COMPLETIONS = "chat_completions"  # /v1/chat/completions
    RESPONSES = "responses"                 # /v1/responses


class ExecutionMode(Enum):
    """
    Execution environment for LLM calls.
    """
    SNOWFLAKE_UDF = "snowflake_udf"      # Running inside Snowflake (UDFs)
    EXTERNAL_HTTP = "external_http"       # Running externally (GitHub Actions/Colab)
    HYBRID = "hybrid"                     # Data from Snowflake, LLM calls external


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class LLMError:
    """
    Structured error information from LLM API calls.
    
    LESSON: Errors can come as dict or string, need flexible handling.
    API can return {"error": null} which needs truthy check.
    """
    message: str
    code: Optional[str] = None
    type: Optional[str] = None
    param: Optional[str] = None
    is_rate_limit: bool = False
    is_timeout: bool = False
    is_quota_exceeded: bool = False
    raw_error: Optional[Any] = None
    
    @classmethod
    def from_api_response(cls, error_data: Any) -> 'LLMError':
        """
        Parse error from API response.
        
        LESSON: Handle both dict and string error formats.
        Check for truthy value first - API can return {"error": null}.
        """
        if not error_data:
            return cls(message="Unknown error (null response)")
        
        if isinstance(error_data, str):
            # String error message
            error_lower = error_data.lower()
            return cls(
                message=error_data,
                is_rate_limit='rate_limit' in error_lower or 'rate limit' in error_lower,
                is_timeout='timeout' in error_lower,
                is_quota_exceeded='quota' in error_lower or 'exceeded' in error_lower,
                raw_error=error_data
            )
        
        if isinstance(error_data, dict):
            # Dict error object
            message = error_data.get('message', str(error_data))
            message_lower = message.lower() if message else ''
            
            return cls(
                message=message,
                code=error_data.get('code'),
                type=error_data.get('type'),
                param=error_data.get('param'),
                is_rate_limit='rate_limit' in message_lower or error_data.get('code') == 'rate_limit_exceeded',
                is_timeout='timeout' in message_lower,
                is_quota_exceeded='quota' in message_lower or 'exceeded' in message_lower,
                raw_error=error_data
            )
        
        return cls(message=str(error_data), raw_error=error_data)


@dataclass
class LLMResponse:
    """
    Standardized response from LLM API calls.
    
    LESSON: Response parsing varies by API type:
    - Chat Completions: choices[0].message.content
    - Responses API: output_text OR output[].content[].text
    """
    conversation_id: str
    success: bool
    response: Optional[str] = None
    error: Optional[LLMError] = None
    
    # Token usage
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    
    # Metadata
    model: str = ""
    api_type: APIType = APIType.CHAT_COMPLETIONS
    latency_ms: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Raw data for debugging
    raw_response: Optional[Dict] = None
    
    @property
    def usage(self) -> Dict[str, int]:
        """Return usage as dict (compatible with existing code)."""
        return {
            'prompt_tokens': self.prompt_tokens,
            'completion_tokens': self.completion_tokens,
            'total_tokens': self.total_tokens
        }
    
    @property
    def tokens_breakdown(self) -> str:
        """Return usage as JSON string (for Snowflake storage)."""
        return json.dumps(self.usage)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'conversation_id': self.conversation_id,
            'success': self.success,
            'response': self.response,
            'error': self.error.message if self.error else None,
            'prompt_tokens': self.prompt_tokens,
            'completion_tokens': self.completion_tokens,
            'total_tokens': self.total_tokens,
            'model': self.model,
            'api_type': self.api_type.value,
            'latency_ms': self.latency_ms,
        }


# =============================================================================
# MODEL CONFIGURATION
# =============================================================================

class ModelConfig:
    """
    Model-specific configuration and API routing.
    
    LESSON: Different models require different APIs and parameters.
    gpt-5 models:
    - Use Responses API (/v1/responses)
    - Don't support temperature parameter
    - Response format differs (output_text vs choices)
    """
    
    # Models that use Responses API
    RESPONSES_API_MODELS = frozenset({
        'gpt-5', 'gpt-5-mini', 'gpt-5-nano',
        'o1', 'o1-mini', 'o1-preview',
        'o3', 'o3-mini'  # Future models
    })
    
    # Models that don't support temperature
    NO_TEMPERATURE_MODELS = frozenset({
        'gpt-5', 'gpt-5-mini', 'gpt-5-nano',
        'o1', 'o1-mini', 'o1-preview',
        'o3', 'o3-mini'
    })
    
    @classmethod
    def get_api_type(cls, model: str) -> APIType:
        """
        Determine which API to use for a given model.
        
        LESSON: gpt-5 family uses Responses API, discovered when
        all Colab calls failed with 0% success using Chat Completions.
        """
        model_lower = model.lower()
        
        # Check exact match
        if model_lower in cls.RESPONSES_API_MODELS:
            return APIType.RESPONSES
        
        # Check prefix match (e.g., gpt-5-turbo-preview)
        if any(model_lower.startswith(m) for m in ['gpt-5', 'o1', 'o3']):
            return APIType.RESPONSES
        
        return APIType.CHAT_COMPLETIONS
    
    @classmethod
    def supports_temperature(cls, model: str) -> bool:
        """
        Check if model supports temperature parameter.
        
        LESSON: gpt-5 models return error:
        "Unsupported parameter: 'temperature' is not supported with this model"
        """
        model_lower = model.lower()
        
        if model_lower in cls.NO_TEMPERATURE_MODELS:
            return False
        
        if any(model_lower.startswith(m) for m in ['gpt-5', 'o1', 'o3']):
            return False
        
        return True
    
    @classmethod
    def get_max_tokens_param_name(cls, model: str) -> str:
        """
        Get the correct parameter name for max tokens.
        
        Chat Completions: max_tokens
        Responses API: max_output_tokens
        """
        if cls.get_api_type(model) == APIType.RESPONSES:
            return 'max_output_tokens'
        return 'max_tokens'


# =============================================================================
# RESPONSE PARSER
# =============================================================================

class ResponseParser:
    """
    Parse LLM API responses into standardized format.
    
    LESSON: Response formats differ significantly:
    - Chat Completions: {"choices": [{"message": {"content": "..."}}]}
    - Responses API: {"output_text": "..."} or {"output": [{"type": "message", "content": [...]}]}
    """
    
    @classmethod
    def parse_response(cls, raw_response: Dict, model: str, conversation_id: str) -> LLMResponse:
        """
        Parse API response into LLMResponse.
        
        LESSON: Must handle both API formats and various error conditions.
        """
        api_type = ModelConfig.get_api_type(model)
        
        # Check for errors first (LESSON: check truthy, can be null)
        if 'error' in raw_response and raw_response['error']:
            error = LLMError.from_api_response(raw_response['error'])
            return LLMResponse(
                conversation_id=conversation_id,
                success=False,
                error=error,
                model=model,
                api_type=api_type,
                raw_response=raw_response
            )
        
        # Parse response text based on API type
        response_text = None
        
        if api_type == APIType.RESPONSES:
            response_text = cls._parse_responses_api(raw_response)
        else:
            response_text = cls._parse_chat_completions_api(raw_response)
        
        # Parse usage
        usage = raw_response.get('usage', {}) or {}
        
        return LLMResponse(
            conversation_id=conversation_id,
            success=response_text is not None,
            response=response_text,
            prompt_tokens=usage.get('prompt_tokens', usage.get('input_tokens', 0)),
            completion_tokens=usage.get('completion_tokens', usage.get('output_tokens', 0)),
            total_tokens=usage.get('total_tokens', 0),
            model=model,
            api_type=api_type,
            raw_response=raw_response
        )
    
    @classmethod
    def _parse_responses_api(cls, response: Dict) -> Optional[str]:
        """
        Parse Responses API format.
        
        LESSON: Try multiple extraction methods:
        1. Direct output_text field
        2. output array with type="message" containing content array
        3. output array with direct content field
        """
        # Method 1: Direct output_text field
        output_text = response.get('output_text')
        if isinstance(output_text, str) and output_text.strip():
            return output_text.strip()
        
        # Method 2: Parse output array
        output = response.get('output', [])
        if not isinstance(output, list):
            return None
        
        parts = []
        for item in output:
            if not isinstance(item, dict):
                continue
            
            # Check for message type with content array
            if item.get('type') == 'message':
                for content in item.get('content', []):
                    if isinstance(content, dict):
                        text = content.get('text')
                        if isinstance(text, str) and text.strip():
                            parts.append(text.strip())
            
            # Check for direct content field
            elif 'content' in item:
                content = item['content']
                if isinstance(content, str) and content.strip():
                    parts.append(content.strip())
        
        if parts:
            return '\n'.join(parts)
        
        return None
    
    @classmethod
    def _parse_chat_completions_api(cls, response: Dict) -> Optional[str]:
        """Parse Chat Completions API format."""
        try:
            choices = response.get('choices', [])
            if choices and len(choices) > 0:
                message = choices[0].get('message', {})
                content = message.get('content')
                if isinstance(content, str):
                    return content.strip()
        except (KeyError, IndexError, TypeError):
            pass
        return None


# =============================================================================
# JSON PARSER UTILITY
# =============================================================================

class JSONParser:
    """
    Utility for parsing JSON from LLM responses.
    
    LLM responses often contain JSON that may have:
    - Leading/trailing text
    - Markdown code blocks
    - Invalid JSON that needs fixing
    """
    
    @classmethod
    def safe_parse(cls, text: str) -> Optional[Union[Dict, List]]:
        """
        Safely parse JSON from LLM response text.
        
        Returns None if parsing fails.
        """
        if not isinstance(text, str) or not text.strip():
            return None
        
        text = text.strip()
        
        # Try direct parse first
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        
        # Try to extract JSON from markdown code block
        code_block_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', text)
        if code_block_match:
            try:
                return json.loads(code_block_match.group(1))
            except json.JSONDecodeError:
                pass
        
        # Try to find JSON object or array
        for pattern in [r'\{[\s\S]*\}', r'\[[\s\S]*\]']:
            match = re.search(pattern, text)
            if match:
                try:
                    return json.loads(match.group(0))
                except json.JSONDecodeError:
                    pass
        
        return None
    
    @classmethod
    def parse_boolean(cls, value: Any) -> Optional[bool]:
        """Parse various boolean representations."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ('true', 'yes', '1')
        if isinstance(value, (int, float)):
            return bool(value)
        return None


# =============================================================================
# ABSTRACT INTERFACE
# =============================================================================

class LLMClientInterface(ABC):
    """
    Abstract interface for LLM API clients.
    
    All LLM clients (Snowflake UDF, External HTTP) must implement this interface
    to ensure consistent behavior across execution modes.
    """
    
    @abstractmethod
    def call_single(
        self,
        conversation_id: str,
        content: str,
        system_prompt: str,
        model: str = 'gpt-4o-mini',
        temperature: float = 0.0,
        max_tokens: int = 2048
    ) -> LLMResponse:
        """
        Make a single LLM call.
        
        Args:
            conversation_id: Unique identifier for the conversation
            content: User message content
            system_prompt: System prompt for the LLM
            model: Model to use
            temperature: Temperature setting (ignored for gpt-5 models)
            max_tokens: Maximum tokens in response
        
        Returns:
            LLMResponse with result or error
        """
        pass
    
    @abstractmethod
    def call_batch(
        self,
        items: List[Dict[str, str]],
        system_prompt: str,
        model: str = 'gpt-4o-mini',
        temperature: float = 0.0,
        max_tokens: int = 2048,
        verbose: bool = True
    ) -> List[LLMResponse]:
        """
        Make batch LLM calls.
        
        Args:
            items: List of {'conversation_id': str, 'content': str}
            system_prompt: System prompt for all calls
            model: Model to use
            temperature: Temperature setting (ignored for gpt-5 models)
            max_tokens: Maximum tokens in response
            verbose: Whether to print progress
        
        Returns:
            List of LLMResponse objects
        """
        pass
    
    @abstractmethod
    def get_execution_mode(self) -> ExecutionMode:
        """Return the execution mode of this client."""
        pass
    
    def get_api_type_for_model(self, model: str) -> APIType:
        """Get the API type for a given model."""
        return ModelConfig.get_api_type(model)
    
    def model_supports_temperature(self, model: str) -> bool:
        """Check if model supports temperature parameter."""
        return ModelConfig.supports_temperature(model)
