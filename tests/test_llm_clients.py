"""
Tests for LLM client implementations.
"""
import pytest
import json
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestAsyncOpenAIClientAPI:
    """Test AsyncOpenAIClient API selection logic."""
    
    def test_chat_api_models(self):
        """Test that gpt-4 models use Chat Completions API."""
        # Import from deployment folder
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                         '..', 'LLM_JUDGE_METRICS_TESTING', 'deployment'))
        try:
            from colab_adapter import AsyncOpenAIClient
            
            client = AsyncOpenAIClient(api_key='test-key')
            
            # These models should NOT use Responses API
            assert not client._uses_responses_api('gpt-4o-mini')
            assert not client._uses_responses_api('gpt-4o')
            assert not client._uses_responses_api('gpt-4')
            assert not client._uses_responses_api('gpt-3.5-turbo')
        except ImportError:
            pytest.skip("colab_adapter not available")
    
    def test_responses_api_models(self):
        """Test that gpt-5 models use Responses API."""
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                         '..', 'LLM_JUDGE_METRICS_TESTING', 'deployment'))
        try:
            from colab_adapter import AsyncOpenAIClient
            
            client = AsyncOpenAIClient(api_key='test-key')
            
            # These models should use Responses API
            assert client._uses_responses_api('gpt-5')
            assert client._uses_responses_api('gpt-5-mini')
            assert client._uses_responses_api('gpt-5-nano')
            assert client._uses_responses_api('o1')
            assert client._uses_responses_api('o1-mini')
        except ImportError:
            pytest.skip("colab_adapter not available")


class TestLLMResponseParsing:
    """Test LLM response parsing."""
    
    def test_parse_chat_completion_response(self, sample_llm_response):
        """Test parsing Chat Completions API response."""
        # Extract response text from standard format
        response_text = sample_llm_response['choices'][0]['message']['content']
        
        assert response_text == '{"ClientSuspectingAI": false}'
        
        # Parse as JSON
        parsed = json.loads(response_text)
        assert parsed['ClientSuspectingAI'] == False
    
    def test_parse_responses_api_response(self, sample_responses_api_response):
        """Test parsing Responses API response format."""
        # Method 1: output_text
        response_text = sample_responses_api_response.get('output_text', '')
        if response_text:
            parsed = json.loads(response_text)
            assert parsed['ClientSuspectingAI'] == True
        
        # Method 2: output array
        output = sample_responses_api_response.get('output', [])
        if output:
            for item in output:
                if item.get('type') == 'message':
                    for content in item.get('content', []):
                        text = content.get('text', '')
                        if text:
                            parsed = json.loads(text)
                            assert 'ClientSuspectingAI' in parsed
    
    def test_parse_json_from_text(self):
        """Test parsing JSON from various text formats."""
        test_cases = [
            ('{"result": true}', True),
            ('  {"result": true}  ', True),
            ('{"result": false}', False),
        ]
        
        for text, expected in test_cases:
            parsed = json.loads(text.strip())
            assert parsed['result'] == expected
    
    def test_handle_markdown_code_block(self):
        """Test handling JSON wrapped in markdown code blocks."""
        text_with_markdown = '```json\n{"result": true}\n```'
        
        # Extract JSON from code block
        if text_with_markdown.strip().startswith('```'):
            lines = text_with_markdown.strip().split('\n')
            json_lines = []
            in_block = False
            for line in lines:
                if line.startswith('```') and not in_block:
                    in_block = True
                    continue
                elif line.startswith('```') and in_block:
                    break
                elif in_block:
                    json_lines.append(line)
            json_text = '\n'.join(json_lines)
        else:
            json_text = text_with_markdown
        
        parsed = json.loads(json_text)
        assert parsed['result'] == True


class TestUsageTracking:
    """Test token usage tracking."""
    
    def test_extract_usage_from_chat_response(self, sample_llm_response):
        """Test extracting usage from Chat Completions response."""
        usage = sample_llm_response.get('usage', {})
        
        assert usage.get('prompt_tokens') == 100
        assert usage.get('completion_tokens') == 20
        assert usage.get('total_tokens') == 120
    
    def test_extract_usage_from_responses_api(self, sample_responses_api_response):
        """Test extracting usage from Responses API response."""
        usage = sample_responses_api_response.get('usage', {})
        
        assert usage.get('input_tokens') == 100
        assert usage.get('output_tokens') == 20
        assert usage.get('total_tokens') == 120
    
    def test_build_tokens_breakdown(self):
        """Test building tokens breakdown JSON."""
        usage = {
            'prompt_tokens': 100,
            'completion_tokens': 50,
            'total_tokens': 150
        }
        
        breakdown = json.dumps({
            'prompt_tokens': usage.get('prompt_tokens', 0),
            'completion_tokens': usage.get('completion_tokens', 0),
            'total_tokens': usage.get('total_tokens', 0)
        })
        
        parsed = json.loads(breakdown)
        assert parsed['prompt_tokens'] == 100
        assert parsed['completion_tokens'] == 50
        assert parsed['total_tokens'] == 150


class TestErrorHandling:
    """Test error handling in LLM clients."""
    
    def test_handle_api_error_response(self):
        """Test handling API error responses."""
        error_response = {
            'error': {
                'message': 'Rate limit exceeded',
                'type': 'rate_limit_error',
                'code': 'rate_limit'
            }
        }
        
        error = error_response.get('error')
        if isinstance(error, dict):
            error_msg = error.get('message', str(error))
        else:
            error_msg = str(error)
        
        assert 'Rate limit' in error_msg
    
    def test_handle_none_error(self):
        """Test handling None error field."""
        response = {'error': None, 'output_text': 'result'}
        
        # Should not raise error
        if response.get('error'):
            error = response['error']
        else:
            result = response.get('output_text')
            assert result == 'result'
    
    def test_handle_string_error(self):
        """Test handling string error."""
        response = {'error': 'Something went wrong'}
        
        error = response.get('error')
        if isinstance(error, dict):
            error_msg = error.get('message', str(error))
        else:
            error_msg = str(error)
        
        assert error_msg == 'Something went wrong'


class TestBatchProcessing:
    """Test batch processing logic."""
    
    def test_batch_size_calculation(self):
        """Test batch size calculation."""
        items = list(range(25))
        batch_size = 10
        
        batches = []
        for i in range(0, len(items), batch_size):
            batches.append(items[i:i+batch_size])
        
        assert len(batches) == 3
        assert len(batches[0]) == 10
        assert len(batches[1]) == 10
        assert len(batches[2]) == 5
    
    def test_empty_batch(self):
        """Test handling empty item list."""
        items = []
        batch_size = 10
        
        batches = []
        for i in range(0, len(items), batch_size):
            batches.append(items[i:i+batch_size])
        
        assert len(batches) == 0
    
    def test_single_item_batch(self):
        """Test single item batch."""
        items = [{'id': '1', 'content': 'test'}]
        batch_size = 10
        
        batches = []
        for i in range(0, len(items), batch_size):
            batches.append(items[i:i+batch_size])
        
        assert len(batches) == 1
        assert len(batches[0]) == 1
