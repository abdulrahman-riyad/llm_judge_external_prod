# =============================================================================
# SNOWFLAKE UDF CLIENT
# =============================================================================
#
# Client that wraps existing Snowflake UDF calls for LLM operations.
# This maintains backward compatibility with the existing Snowflake implementation.
#
# =============================================================================

import json
import time
from typing import Dict, List, Any, Optional

from .interface import (
    LLMClientInterface,
    LLMResponse,
    LLMError,
    ExecutionMode,
    APIType,
    ModelConfig,
    JSONParser
)


class SnowflakeUDFClient(LLMClientInterface):
    """
    LLM client that uses Snowflake UDFs for API calls.
    
    This wraps the existing openai_chat_system UDF to provide
    a consistent interface with the async HTTP client.
    
    Note: This client makes sequential calls (no parallelism)
    because Snowflake UDFs are inherently synchronous.
    """
    
    def __init__(self, session):
        """
        Initialize with Snowflake session.
        
        Args:
            session: Snowflake Snowpark Session object
        """
        self.session = session
    
    def get_execution_mode(self) -> ExecutionMode:
        return ExecutionMode.SNOWFLAKE_UDF
    
    def call_single(
        self,
        conversation_id: str,
        content: str,
        system_prompt: str,
        model: str = 'gpt-4o-mini',
        temperature: float = 0.0,
        max_tokens: int = 2048,
        department: str = None,
        api_key_alias: str = None
    ) -> LLMResponse:
        """
        Make a single LLM call using Snowflake UDF.
        
        Args:
            conversation_id: Unique identifier for tracking
            content: User message content
            system_prompt: System prompt for the LLM
            model: Model to use
            temperature: Temperature setting
            max_tokens: Maximum tokens in response
            department: Department name (for API key routing)
            api_key_alias: Direct API key alias override
        
        Returns:
            LLMResponse with result or error
        """
        start_time = time.time()
        
        # Determine API key alias from department if not provided
        if api_key_alias is None:
            api_key_alias = self._get_api_key_alias(department)
        
        try:
            # Build and execute UDF call
            # Note: The UDF handles API routing internally
            result = self._execute_udf(
                content=content,
                system_prompt=system_prompt,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                api_key_alias=api_key_alias
            )
            
            latency_ms = int((time.time() - start_time) * 1000)
            
            return self._parse_udf_result(result, conversation_id, model, latency_ms)
            
        except Exception as e:
            latency_ms = int((time.time() - start_time) * 1000)
            return LLMResponse(
                conversation_id=conversation_id,
                success=False,
                error=LLMError(message=f"UDF execution error: {str(e)}"),
                model=model,
                api_type=ModelConfig.get_api_type(model),
                latency_ms=latency_ms
            )
    
    def call_batch(
        self,
        items: List[Dict[str, str]],
        system_prompt: str,
        model: str = 'gpt-4o-mini',
        temperature: float = 0.0,
        max_tokens: int = 2048,
        verbose: bool = True,
        department: str = None
    ) -> List[LLMResponse]:
        """
        Make batch LLM calls using Snowflake UDFs.
        
        Note: Calls are made sequentially (no parallelism in Snowflake UDFs).
        This is the existing behavior that incurs Snowflake compute costs.
        
        Args:
            items: List of {'conversation_id': str, 'content': str}
            system_prompt: System prompt for all calls
            model: Model to use
            temperature: Temperature setting
            max_tokens: Maximum tokens in response
            verbose: Whether to print progress
            department: Department name (for API key routing)
        
        Returns:
            List of LLMResponse objects
        """
        results = []
        total = len(items)
        
        for i, item in enumerate(items):
            if verbose and (i + 1) % 10 == 0:
                print(f"      Processing {i + 1}/{total}...")
            
            result = self.call_single(
                conversation_id=item['conversation_id'],
                content=item['content'],
                system_prompt=system_prompt,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                department=department
            )
            results.append(result)
        
        if verbose:
            successful = sum(1 for r in results if r.success)
            print(f"      ✅ Completed: {successful}/{total} successful")
        
        return results
    
    def _execute_udf(
        self,
        content: str,
        system_prompt: str,
        model: str,
        temperature: float,
        max_tokens: int,
        api_key_alias: str
    ) -> Dict[str, Any]:
        """
        Execute the openai_chat_system UDF.
        
        This matches the signature of the UDF defined in llms_udfs.sql
        """
        # Escape single quotes in content and prompt for SQL
        content_escaped = content.replace("'", "''")
        prompt_escaped = system_prompt.replace("'", "''")
        
        # Build SQL query
        # The UDF signature: openai_chat_system(content, system_prompt, model, temp, max_tokens, output_mode, conv_id, mode, api_key_alias, extra)
        sql = f"""
        SELECT OPENAI_CHAT_SYSTEM(
            '{content_escaped}',
            '{prompt_escaped}',
            '{model}',
            {temperature},
            {max_tokens},
            'full',
            'batch_call',
            'batch',
            '{api_key_alias}',
            NULL
        ) AS RESPONSE
        """
        
        # Execute query
        result = self.session.sql(sql).collect()
        
        if result and len(result) > 0:
            response_str = result[0]['RESPONSE']
            if isinstance(response_str, str):
                return json.loads(response_str)
            return response_str
        
        return {'error': 'No result from UDF'}
    
    def _parse_udf_result(
        self,
        result: Dict[str, Any],
        conversation_id: str,
        model: str,
        latency_ms: int
    ) -> LLMResponse:
        """
        Parse UDF result into LLMResponse.
        
        The UDF returns a JSON object with:
        - text: The LLM response text
        - error: Error message if any
        - usage: Token usage breakdown
        """
        api_type = ModelConfig.get_api_type(model)
        
        # Check for errors
        if 'error' in result and result['error']:
            error_data = result['error']
            return LLMResponse(
                conversation_id=conversation_id,
                success=False,
                error=LLMError.from_api_response(error_data),
                model=model,
                api_type=api_type,
                latency_ms=latency_ms,
                raw_response=result
            )
        
        # Extract response text
        response_text = result.get('text', '')
        
        # Extract usage
        usage = result.get('usage', {}) or {}
        
        return LLMResponse(
            conversation_id=conversation_id,
            success=bool(response_text),
            response=response_text,
            prompt_tokens=usage.get('input_tokens', usage.get('prompt_tokens', 0)),
            completion_tokens=usage.get('output_tokens', usage.get('completion_tokens', 0)),
            total_tokens=usage.get('total_tokens', 0),
            model=model,
            api_type=api_type,
            latency_ms=latency_ms,
            raw_response=result
        )
    
    def _get_api_key_alias(self, department: str = None) -> str:
        """
        Get API key alias for a department.
        
        Maps department names to Snowflake secret aliases.
        Matches the logic in the existing codebase.
        """
        # Department to alias mapping
        DEPARTMENT_ALIASES = {
            'CC_Resolvers': 'OPENAI_KEY',
            'CC_Delighters': 'OPENAI_KEY',
            'MV_Resolvers': 'OPENAI_KEY',
            'MV_Sales': 'OPENAI_KEY',
            'MV_Collect_Info': 'OPENAI_KEY',
            'AT_African': 'OPENAI_KEY',
            # Add more as needed
        }
        
        if department and department in DEPARTMENT_ALIASES:
            return DEPARTMENT_ALIASES[department]
        
        return 'OPENAI_KEY'  # Default alias
    
    def execute_raw_sql(self, sql: str) -> List[Any]:
        """
        Execute raw SQL and return results.
        
        Useful for custom queries or debugging.
        """
        return self.session.sql(sql).collect()
