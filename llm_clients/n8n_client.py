"""
N8N API Client for External Execution

This module provides direct HTTP access to N8N API for fetching
client attributes and chat history, replacing the Snowflake UDF
`GET_N8N_CLIENT_ATTRS_AND_HISTORY` for external execution environments.

Used by tool_eval message-level processing.
"""

import json
import asyncio
import aiohttp
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class N8NData:
    """Container for N8N execution data."""
    execution_id: str
    client_attributes: Dict[str, Any]
    history: List[Dict[str, str]]
    success: bool
    error: Optional[str] = None


class N8NClient:
    """
    Async client for fetching data from N8N API.
    
    Mirrors the functionality of GET_N8N_CLIENT_ATTRS_AND_HISTORY UDF
    for use in external execution environments (Colab, GitHub Actions, etc.)
    """
    
    BASE_URL = "https://n8n.teljoy.io/api/v1/executions"
    TIMEOUT = 30
    MAX_RETRIES = 3
    BATCH_SIZE = 20
    DELAY_BETWEEN_BATCHES = 0.5
    
    def __init__(self, api_key: str):
        """
        Initialize N8N client.
        
        Args:
            api_key: N8N API key (X-N8N-API-KEY header)
        """
        self.api_key = api_key
    
    def _extract_client_attributes(self, run_data: Dict) -> Dict[str, Any]:
        """
        Extract client attributes from N8N run data.
        
        Args:
            run_data: N8N execution runData object
            
        Returns:
            Client attributes dictionary
        """
        try:
            # Primary path: full_data -> data -> main -> json
            full_data = run_data.get('full_data', [{}])
            if full_data:
                main_data = full_data[0].get('data', {}).get('main', [])
                if main_data and len(main_data) > 0:
                    first_item = main_data[0]
                    if isinstance(first_item, list) and len(first_item) > 0:
                        return first_item[0].get('json', {})
                    elif isinstance(first_item, dict):
                        return first_item.get('json', {})
        except (IndexError, KeyError, TypeError):
            pass
        
        return {}
    
    def _extract_chat_history(self, run_data: Dict) -> List[Dict[str, str]]:
        """
        Extract chat history from N8N run data.
        Converts from N8N format to langchain format.
        
        Args:
            run_data: N8N execution runData object
            
        Returns:
            List of chat history entries in langchain format
        """
        history = []
        
        try:
            # Try primary path: Redis Chat Memory
            chat_history = None
            
            for memory_key in ['Redis Chat Memory', 'Redis Chat Memory1']:
                try:
                    memory_data = run_data.get(memory_key, [{}])
                    if memory_data:
                        ai_memory = memory_data[0].get('data', {}).get('ai_memory', [])
                        if ai_memory:
                            first_ai = ai_memory[0]
                            if isinstance(first_ai, list) and len(first_ai) > 0:
                                chat_history = first_ai[0].get('json', {}).get('chatHistory')
                            elif isinstance(first_ai, dict):
                                chat_history = first_ai.get('json', {}).get('chatHistory')
                        
                        if isinstance(chat_history, list):
                            break
                except (IndexError, KeyError, TypeError):
                    continue
            
            if isinstance(chat_history, list):
                # Convert to langchain format
                for entry in chat_history:
                    if isinstance(entry, dict):
                        role = entry.get('role', entry.get('type', 'unknown'))
                        content = entry.get('content', entry.get('text', ''))
                        
                        # Normalize role names
                        if role in ['human', 'user', 'consumer']:
                            role = 'human'
                        elif role in ['ai', 'assistant', 'bot']:
                            role = 'ai'
                        
                        if content:
                            history.append({
                                'role': role,
                                'content': str(content)
                            })
        
        except Exception:
            pass
        
        return history
    
    async def _fetch_execution(
        self,
        session: aiohttp.ClientSession,
        execution_id: str
    ) -> N8NData:
        """
        Fetch single execution data from N8N API.
        
        Args:
            session: aiohttp session
            execution_id: N8N execution ID
            
        Returns:
            N8NData object with client_attributes and history
        """
        if not execution_id:
            return N8NData(
                execution_id=execution_id or '',
                client_attributes={},
                history=[],
                success=False,
                error='Empty execution_id'
            )
        
        # Clean execution_id (might be float like 586708.0)
        try:
            exec_id_str = str(int(float(execution_id)))
        except (ValueError, TypeError):
            exec_id_str = str(execution_id)
        
        url = f"{self.BASE_URL}/{exec_id_str}"
        headers = {"X-N8N-API-KEY": self.api_key}
        
        for attempt in range(self.MAX_RETRIES):
            try:
                async with session.get(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=self.TIMEOUT)
                ) as response:
                    if response.status != 200:
                        if attempt < self.MAX_RETRIES - 1:
                            await asyncio.sleep(1)
                            continue
                        return N8NData(
                            execution_id=exec_id_str,
                            client_attributes={},
                            history=[],
                            success=False,
                            error=f'HTTP {response.status}'
                        )
                    
                    data = await response.json()
                    run_data = data.get('data', {}).get('resultData', {}).get('runData', {})
                    
                    client_attrs = self._extract_client_attributes(run_data)
                    history = self._extract_chat_history(run_data)
                    
                    return N8NData(
                        execution_id=exec_id_str,
                        client_attributes=client_attrs,
                        history=history,
                        success=True
                    )
                    
            except asyncio.TimeoutError:
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(1)
                    continue
                return N8NData(
                    execution_id=exec_id_str,
                    client_attributes={},
                    history=[],
                    success=False,
                    error='Timeout'
                )
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(1)
                    continue
                return N8NData(
                    execution_id=exec_id_str,
                    client_attributes={},
                    history=[],
                    success=False,
                    error=str(e)
                )
        
        return N8NData(
            execution_id=exec_id_str,
            client_attributes={},
            history=[],
            success=False,
            error='Max retries exceeded'
        )
    
    async def _fetch_batch(
        self,
        execution_ids: List[str]
    ) -> List[N8NData]:
        """
        Fetch multiple executions in parallel.
        
        Args:
            execution_ids: List of execution IDs
            
        Returns:
            List of N8NData objects
        """
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_execution(session, exec_id)
                for exec_id in execution_ids
            ]
            return await asyncio.gather(*tasks)
    
    def fetch_all(
        self,
        execution_ids: List[str],
        verbose: bool = True
    ) -> Dict[str, N8NData]:
        """
        Fetch N8N data for all execution IDs with batching.
        
        Args:
            execution_ids: List of execution IDs
            verbose: Print progress
            
        Returns:
            Dict mapping execution_id -> N8NData
        """
        results = {}
        total_batches = (len(execution_ids) + self.BATCH_SIZE - 1) // self.BATCH_SIZE
        
        for i in range(0, len(execution_ids), self.BATCH_SIZE):
            batch = execution_ids[i:i + self.BATCH_SIZE]
            batch_num = i // self.BATCH_SIZE + 1
            
            if verbose:
                print(f"      📡 N8N batch {batch_num}/{total_batches} ({len(batch)} items)...")
            
            batch_results = asyncio.run(self._fetch_batch(batch))
            
            for n8n_data in batch_results:
                results[n8n_data.execution_id] = n8n_data
            
            successful = sum(1 for r in batch_results if r.success)
            if verbose:
                print(f"      ✅ {successful}/{len(batch)} fetched")
            
            if i + self.BATCH_SIZE < len(execution_ids):
                import time
                time.sleep(self.DELAY_BETWEEN_BATCHES)
        
        return results


# Global client instance
_n8n_client: Optional[N8NClient] = None


def set_n8n_client(api_key: str) -> None:
    """Initialize the global N8N client."""
    global _n8n_client
    _n8n_client = N8NClient(api_key)
    print(f"✅ N8N client initialized")


def get_n8n_client() -> Optional[N8NClient]:
    """Get the global N8N client (or None if not initialized)."""
    return _n8n_client


def is_n8n_client_available() -> bool:
    """Check if N8N client is available."""
    return _n8n_client is not None
