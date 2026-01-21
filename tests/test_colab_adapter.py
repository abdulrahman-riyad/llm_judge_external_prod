"""
Tests for Colab adapter components.
"""
import pytest
import pandas as pd
import json
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestColabSnowflakeSession:
    """Test ColabSnowflakeSession mock functionality."""
    
    def test_session_creation(self):
        """Test creating a mock session."""
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                         '..', 'LLM_JUDGE_METRICS_TESTING', 'deployment'))
        try:
            from colab_adapter import ColabSnowflakeSession
            
            config = {
                'account': 'test_account',
                'user': 'test_user',
                'password': 'test_pass',
                'warehouse': 'test_wh',
                'database': 'test_db',
                'schema': 'test_schema',
                'role': 'test_role'
            }
            
            # Should not raise (doesn't connect until needed)
            session = ColabSnowflakeSession(config)
            assert session.config == config
        except ImportError:
            pytest.skip("colab_adapter not available")


class TestColabRow:
    """Test ColabRow mock functionality."""
    
    def test_row_attribute_access(self):
        """Test accessing row data as attributes."""
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                         '..', 'LLM_JUDGE_METRICS_TESTING', 'deployment'))
        try:
            from colab_adapter import ColabRow
            
            series = pd.Series({'NAME': 'John', 'AGE': 30, 'CITY': 'NYC'})
            row = ColabRow(series)
            
            assert row.NAME == 'John'
            assert row.AGE == 30
            assert row.CITY == 'NYC'
        except ImportError:
            pytest.skip("colab_adapter not available")
    
    def test_row_dict_access(self):
        """Test accessing row data as dict."""
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                         '..', 'LLM_JUDGE_METRICS_TESTING', 'deployment'))
        try:
            from colab_adapter import ColabRow
            
            series = pd.Series({'NAME': 'John', 'VALUE': 100})
            row = ColabRow(series)
            
            assert row['NAME'] == 'John'
            assert row['VALUE'] == 100
        except ImportError:
            pytest.skip("colab_adapter not available")
    
    def test_row_as_dict(self):
        """Test converting row to dict."""
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                         '..', 'LLM_JUDGE_METRICS_TESTING', 'deployment'))
        try:
            from colab_adapter import ColabRow
            
            series = pd.Series({'A': 1, 'B': 2})
            row = ColabRow(series)
            
            d = row.asDict()
            assert d == {'A': 1, 'B': 2}
        except ImportError:
            pytest.skip("colab_adapter not available")


class TestCallLLMParallel:
    """Test call_llm_parallel function."""
    
    def test_prepares_items_correctly(self, sample_xml_df):
        """Test that items are prepared correctly for LLM calls."""
        items = []
        for _, row in sample_xml_df.iterrows():
            items.append({
                'conversation_id': row['CONVERSATION_ID'],
                'content': str(row['CONVERSATION_XML'])
            })
        
        assert len(items) == 2
        assert items[0]['conversation_id'] == 'conv1'
        assert 'conversation' in items[0]['content'].lower()
    
    def test_handles_empty_dataframe(self):
        """Test handling empty DataFrame."""
        empty_df = pd.DataFrame(columns=['CONVERSATION_ID', 'CONVERSATION_XML'])
        
        items = []
        for _, row in empty_df.iterrows():
            items.append({
                'conversation_id': row['CONVERSATION_ID'],
                'content': str(row['CONVERSATION_XML'])
            })
        
        assert len(items) == 0


class TestResultsMerging:
    """Test results merging functionality."""
    
    def test_merge_results_with_original(self, sample_xml_df):
        """Test merging LLM results with original DataFrame."""
        # Simulate LLM results
        results = [
            {'conversation_id': 'conv1', 'success': True, 'response': '{"result": true}', 'error': None, 'usage': {}},
            {'conversation_id': 'conv2', 'success': True, 'response': '{"result": false}', 'error': None, 'usage': {}}
        ]
        
        results_df = pd.DataFrame(results)
        results_df = results_df.rename(columns={'conversation_id': 'CONVERSATION_ID'})
        
        # Merge
        output_df = sample_xml_df.merge(results_df, on='CONVERSATION_ID', how='left')
        
        assert len(output_df) == 2
        assert 'success' in output_df.columns
        assert 'response' in output_df.columns
    
    def test_handles_missing_results(self, sample_xml_df):
        """Test handling missing results for some conversations."""
        # Only results for conv1
        results = [
            {'conversation_id': 'conv1', 'success': True, 'response': '{"result": true}', 'error': None, 'usage': {}}
        ]
        
        results_df = pd.DataFrame(results)
        results_df = results_df.rename(columns={'conversation_id': 'CONVERSATION_ID'})
        
        # Merge with left join
        output_df = sample_xml_df.merge(results_df, on='CONVERSATION_ID', how='left')
        
        assert len(output_df) == 2
        # conv2 should have NaN for result columns
        conv2_row = output_df[output_df['CONVERSATION_ID'] == 'conv2'].iloc[0]
        assert pd.isna(conv2_row['success'])


class TestTokensBreakdown:
    """Test tokens breakdown JSON handling."""
    
    def test_build_tokens_breakdown_from_usage(self):
        """Test building tokens breakdown JSON from usage dict."""
        usage = {
            'prompt_tokens': 100,
            'completion_tokens': 50,
            'total_tokens': 150
        }
        
        tokens_breakdown = json.dumps({
            'prompt_tokens': usage.get('prompt_tokens', 0),
            'completion_tokens': usage.get('completion_tokens', 0),
            'total_tokens': usage.get('total_tokens', 0)
        })
        
        parsed = json.loads(tokens_breakdown)
        assert parsed['prompt_tokens'] == 100
    
    def test_handle_responses_api_usage_format(self):
        """Test handling Responses API usage format."""
        usage = {
            'input_tokens': 100,
            'output_tokens': 50,
            'total_tokens': 150
        }
        
        # Map to standard format
        tokens_breakdown = json.dumps({
            'prompt_tokens': usage.get('prompt_tokens', usage.get('input_tokens', 0)),
            'completion_tokens': usage.get('completion_tokens', usage.get('output_tokens', 0)),
            'total_tokens': usage.get('total_tokens', 0)
        })
        
        parsed = json.loads(tokens_breakdown)
        assert parsed['prompt_tokens'] == 100
        assert parsed['completion_tokens'] == 50
    
    def test_handle_empty_usage(self):
        """Test handling empty or None usage."""
        usage = None
        
        if usage is None:
            usage = {}
        
        tokens_breakdown = json.dumps({
            'prompt_tokens': usage.get('prompt_tokens', 0),
            'completion_tokens': usage.get('completion_tokens', 0),
            'total_tokens': usage.get('total_tokens', 0)
        })
        
        parsed = json.loads(tokens_breakdown)
        assert parsed['prompt_tokens'] == 0
        assert parsed['completion_tokens'] == 0
