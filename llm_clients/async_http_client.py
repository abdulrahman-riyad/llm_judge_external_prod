# =============================================================================
# ASYNC HTTP CLIENT
# =============================================================================
#
# External HTTP client for making parallel LLM API calls.
# Designed for use in GitHub Actions, Google Colab, or any external environment.
#
# LESSONS LEARNED FROM COLAB DEPLOYMENT (all incorporated):
# 1. Dual API support: Chat Completions vs Responses API
# 2. gpt-5 models don't support temperature parameter
# 3. Robust error handling: check truthy errors, handle dict/string formats
# 4. Flexible response parsing: output_text, output array with type="message"
# 5. Rate limiting with exponential backoff
#
# =============================================================================

import asyncio
import aiohttp
import time
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

from .interface import (
    LLMClientInterface,
    LLMResponse,
    LLMError,
    ExecutionMode,
    APIType,
    ModelConfig,
    ResponseParser
)


class AsyncHTTPClient(LLMClientInterface):
    """
    Async HTTP client for parallel LLM API calls.
    
    Supports both OpenAI API formats:
    - Chat Completions API (/v1/chat/completions) for gpt-4 models
    - Responses API (/v1/responses) for gpt-5 models
    
    Features:
    - Parallel batch processing
    - Automatic API endpoint selection based on model
    - Rate limit handling with exponential backoff
    - Configurable batch size and delays
    """
    
    # API endpoints
    CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
    RESPONSES_URL = "https://api.openai.com/v1/responses"
    
    def __init__(
        self,
        api_key: str,
        api_keys: Dict[str, str] = None,
        batch_size: int = 20,
        delay_between_batches: float = 1.0,
        max_retries: int = 3,
        timeout_seconds: int = 180
    ):
        """
        Initialize the async HTTP client.
        
        Args:
            api_key: Default OpenAI API key
            api_keys: Optional dict of department -> API key for per-department routing
            batch_size: Number of parallel requests per batch
            delay_between_batches: Seconds to wait between batches
            max_retries: Maximum retry attempts per request
            timeout_seconds: Request timeout in seconds
        """
        self._api_key = api_key
        self._api_keys = api_keys or {}
        self._api_keys.setdefault('default', api_key)
        
        self.batch_size = batch_size
        self.delay_between_batches = delay_between_batches
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        
        # For synchronous interface
        self._loop = None
    
    def get_execution_mode(self) -> ExecutionMode:
        return ExecutionMode.EXTERNAL_HTTP
    
    def get_api_key(self, department: str = None) -> str:
        """Get API key for a department."""
        if department and department in self._api_keys:
            return self._api_keys[department]
        return self._api_keys.get('default', self._api_key)
    
    def set_api_key(self, key: str, department: str = None) -> None:
        """Set API key for a department."""
        if department:
            self._api_keys[department] = key
        else:
            self._api_key = key
            self._api_keys['default'] = key
    
    # =========================================================================
    # PUBLIC INTERFACE (Synchronous wrappers)
    # =========================================================================
    
    def call_single(
        self,
        conversation_id: str,
        content: str,
        system_prompt: str,
        model: str = 'gpt-4o-mini',
        temperature: float = 0.0,
        max_tokens: int = 2048,
        department: str = None
    ) -> LLMResponse:
        """Make a single LLM call (synchronous wrapper)."""
        return self._run_async(
            self._call_single_async(
                conversation_id, content, system_prompt,
                model, temperature, max_tokens, department
            )
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
        """Make batch LLM calls (synchronous wrapper)."""
        return self._run_async(
            self._process_all_async(
                items, system_prompt, model, temperature,
                max_tokens, verbose, department
            )
        )
    
    # Alias for compatibility with run_pipeline.py
    def process_all(
        self,
        items: List[Dict[str, str]],
        system_prompt: str,
        model: str = 'gpt-4o-mini',
        temperature: float = 0.0,
        max_tokens: int = 2048,
        verbose: bool = True,
        department: str = None
    ) -> List[LLMResponse]:
        """Alias for call_batch - used by run_pipeline.py."""
        return self.call_batch(
            items, system_prompt, model, temperature,
            max_tokens, verbose, department
        )
    
    # =========================================================================
    # ASYNC IMPLEMENTATION
    # =========================================================================
    
    async def _call_single_async(
        self,
        conversation_id: str,
        content: str,
        system_prompt: str,
        model: str = 'gpt-4o-mini',
        temperature: float = 0.0,
        max_tokens: int = 2048,
        department: str = None
    ) -> LLMResponse:
        """Make a single LLM call asynchronously."""
        api_key = self.get_api_key(department)
        
        async with aiohttp.ClientSession() as session:
            return await self._make_api_call(
                session, conversation_id, content, system_prompt,
                model, temperature, max_tokens, api_key
            )
    
    async def _process_all_async(
        self,
        items: List[Dict[str, str]],
        system_prompt: str,
        model: str = 'gpt-4o-mini',
        temperature: float = 0.0,
        max_tokens: int = 2048,
        verbose: bool = True,
        department: str = None
    ) -> List[LLMResponse]:
        """Process all items with batching and rate limiting."""
        api_key = self.get_api_key(department)
        all_results = []
        total_batches = (len(items) + self.batch_size - 1) // self.batch_size
        
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(items), self.batch_size):
                batch = items[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                
                if verbose:
                    print(f"      📤 Batch {batch_num}/{total_batches} ({len(batch)} items)...")
                
                start_time = time.time()
                
                # Process batch in parallel
                tasks = [
                    self._make_api_call(
                        session,
                        item['conversation_id'],
                        item['content'],
                        system_prompt,
                        model,
                        temperature,
                        max_tokens,
                        api_key
                    )
                    for item in batch
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Convert exceptions to error responses
                processed_results = []
                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        processed_results.append(LLMResponse(
                            conversation_id=batch[j]['conversation_id'],
                            success=False,
                            error=LLMError(message=str(result))
                        ))
                    else:
                        processed_results.append(result)
                
                all_results.extend(processed_results)
                
                elapsed = time.time() - start_time
                successful = sum(1 for r in processed_results if r.success)
                
                if verbose:
                    print(f"      ✅ {successful}/{len(batch)} success in {elapsed:.1f}s")
                    # Show first error if any failures
                    failed = [r for r in processed_results if not r.success]
                    if failed:
                        first_error = failed[0].error.message if failed[0].error else "Unknown"
                        print(f"      ❌ First error: {first_error[:100]}")
                
                # Delay between batches
                if i + self.batch_size < len(items):
                    await asyncio.sleep(self.delay_between_batches)
        
        return all_results
    
    async def _make_api_call(
        self,
        session: aiohttp.ClientSession,
        conversation_id: str,
        content: str,
        system_prompt: str,
        model: str,
        temperature: float,
        max_tokens: int,
        api_key: str
    ) -> LLMResponse:
        """
        Make a single API call with retry logic.
        
        LESSON: Automatically routes to correct API endpoint based on model.
        """
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        # Determine API endpoint and build payload
        api_type = ModelConfig.get_api_type(model)
        url, payload = self._build_request(
            api_type, model, content, system_prompt, temperature, max_tokens
        )
        
        start_time = time.time()
        
        for attempt in range(self.max_retries):
            try:
                async with session.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.timeout_seconds)
                ) as response:
                    raw_response = await response.json()
                    
                    # Parse using our standardized parser
                    result = ResponseParser.parse_response(
                        raw_response, model, conversation_id
                    )
                    result.latency_ms = int((time.time() - start_time) * 1000)
                    
                    # Check for rate limit errors
                    if not result.success and result.error and result.error.is_rate_limit:
                        if attempt < self.max_retries - 1:
                            wait_time = 2 ** attempt
                            await asyncio.sleep(wait_time)
                            continue
                    
                    return result
                    
            except asyncio.TimeoutError:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return LLMResponse(
                    conversation_id=conversation_id,
                    success=False,
                    error=LLMError(message="Request timeout", is_timeout=True),
                    model=model,
                    api_type=api_type,
                    latency_ms=int((time.time() - start_time) * 1000)
                )
                
            except aiohttp.ClientError as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return LLMResponse(
                    conversation_id=conversation_id,
                    success=False,
                    error=LLMError(message=f"HTTP error: {str(e)}"),
                    model=model,
                    api_type=api_type,
                    latency_ms=int((time.time() - start_time) * 1000)
                )
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return LLMResponse(
                    conversation_id=conversation_id,
                    success=False,
                    error=LLMError(message=f"Unexpected error: {str(e)}"),
                    model=model,
                    api_type=api_type,
                    latency_ms=int((time.time() - start_time) * 1000)
                )
        
        # Should not reach here, but just in case
        return LLMResponse(
            conversation_id=conversation_id,
            success=False,
            error=LLMError(message="Max retries exceeded"),
            model=model,
            api_type=api_type,
            latency_ms=int((time.time() - start_time) * 1000)
        )
    
    def _build_request(
        self,
        api_type: APIType,
        model: str,
        content: str,
        system_prompt: str,
        temperature: float,
        max_tokens: int
    ) -> tuple:
        """
        Build API request URL and payload.
        
        LESSONS:
        - gpt-5 models use Responses API with different payload structure
        - gpt-5 models don't support temperature parameter
        """
        if api_type == APIType.RESPONSES:
            # Responses API format
            url = self.RESPONSES_URL
            payload = {
                "model": model,
                "input": content,
                "instructions": system_prompt,
                "max_output_tokens": max_tokens
                # NOTE: temperature NOT included - not supported for gpt-5 models
            }
        else:
            # Chat Completions API format
            url = self.CHAT_COMPLETIONS_URL
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": content}
                ],
                "max_tokens": max_tokens,
                "temperature": temperature
            }
        
        return url, payload
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    
    def _run_async(self, coro):
        """Run async coroutine in sync context."""
        try:
            # Try to get existing event loop
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # In Jupyter/Colab - use nest_asyncio if available
                try:
                    import nest_asyncio
                    nest_asyncio.apply()
                    return loop.run_until_complete(coro)
                except ImportError:
                    # Create new loop in thread
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as pool:
                        return pool.submit(asyncio.run, coro).result()
            else:
                return loop.run_until_complete(coro)
        except RuntimeError:
            # No event loop - create one
            return asyncio.run(coro)
