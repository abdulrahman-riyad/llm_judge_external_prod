"""
Tests for helper functions and utilities.
"""
import pytest
import json
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestJSONParsing:
    """Test JSON parsing utilities."""
    
    def test_parse_valid_json(self):
        """Test parsing valid JSON."""
        text = '{"ClientSuspectingAI": false, "Confidence": 0.9}'
        parsed = json.loads(text)
        
        assert parsed['ClientSuspectingAI'] == False
        assert parsed['Confidence'] == 0.9
    
    def test_parse_json_with_whitespace(self):
        """Test parsing JSON with extra whitespace."""
        text = '  \n  {"result": true}  \n  '
        parsed = json.loads(text.strip())
        
        assert parsed['result'] == True
    
    def test_parse_nested_json(self):
        """Test parsing nested JSON structure."""
        text = '{"outer": {"inner": "value"}, "list": [1, 2, 3]}'
        parsed = json.loads(text)
        
        assert parsed['outer']['inner'] == 'value'
        assert parsed['list'] == [1, 2, 3]
    
    def test_parse_json_array(self):
        """Test parsing JSON array."""
        text = '[{"name": "a"}, {"name": "b"}]'
        parsed = json.loads(text)
        
        assert len(parsed) == 2
        assert parsed[0]['name'] == 'a'
    
    def test_invalid_json_raises_error(self):
        """Test that invalid JSON raises JSONDecodeError."""
        text = '{invalid json}'
        
        with pytest.raises(json.JSONDecodeError):
            json.loads(text)


class TestBooleanParsing:
    """Test boolean value parsing from LLM responses."""
    
    def test_parse_boolean_true_values(self):
        """Test parsing various 'true' representations."""
        true_values = ['true', 'True', 'TRUE', 'yes', 'Yes', 'YES', '1']
        
        for val in true_values:
            result = val.strip().lower() in ('true', 'yes', '1')
            assert result == True, f"Failed for value: {val}"
    
    def test_parse_boolean_false_values(self):
        """Test parsing various 'false' representations."""
        false_values = ['false', 'False', 'FALSE', 'no', 'No', 'NO', '0']
        
        for val in false_values:
            result = val.strip().lower() in ('true', 'yes', '1')
            assert result == False, f"Failed for value: {val}"
    
    def test_parse_boolean_from_dict(self):
        """Test parsing boolean from JSON dict."""
        responses = [
            ('{"ClientSuspectingAI": true}', True),
            ('{"ClientSuspectingAI": false}', False),
            ('{"ClientSuspectingAI": "true"}', True),
            ('{"ClientSuspectingAI": "false"}', False),
        ]
        
        for text, expected in responses:
            parsed = json.loads(text)
            val = parsed.get('ClientSuspectingAI')
            
            if isinstance(val, bool):
                result = val
            elif isinstance(val, str):
                result = val.lower() == 'true'
            else:
                result = bool(val)
            
            assert result == expected, f"Failed for: {text}"


class TestDateHandling:
    """Test date handling utilities."""
    
    def test_date_string_format(self):
        """Test date string formatting."""
        from datetime import date
        
        d = date(2026, 1, 15)
        date_str = d.strftime('%Y-%m-%d')
        
        assert date_str == '2026-01-15'
    
    def test_parse_date_from_string(self):
        """Test parsing date from string."""
        date_str = '2026-01-15'
        d = pd.to_datetime(date_str).date()
        
        assert d.year == 2026
        assert d.month == 1
        assert d.day == 15


class TestDataFrameOperations:
    """Test DataFrame operation utilities."""
    
    def test_groupby_conversation(self, sample_messages_df):
        """Test grouping messages by conversation."""
        grouped = sample_messages_df.groupby('CONVERSATION_ID')
        
        assert len(grouped) == 2
        
        conv1 = grouped.get_group('conv1')
        assert len(conv1) == 3
    
    def test_filter_by_skill(self, sample_messages_df):
        """Test filtering by skill."""
        filtered = sample_messages_df[
            sample_messages_df['SKILL'].str.contains('MV_RESOLVERS', case=False)
        ]
        
        assert len(filtered) == 5  # All rows match
    
    def test_limit_conversations(self, sample_messages_df):
        """Test limiting number of conversations."""
        test_limit = 1
        conv_ids = sample_messages_df['CONVERSATION_ID'].unique()[:test_limit]
        limited_df = sample_messages_df[sample_messages_df['CONVERSATION_ID'].isin(conv_ids)]
        
        assert limited_df['CONVERSATION_ID'].nunique() == 1
    
    def test_count_unique_conversations(self, sample_messages_df):
        """Test counting unique conversations."""
        unique_count = sample_messages_df['CONVERSATION_ID'].nunique()
        
        assert unique_count == 2


class TestSQLQueryBuilding:
    """Test SQL query building utilities."""
    
    def test_build_select_query(self):
        """Test building SELECT query."""
        table = 'SILVER.CHAT_EVALS.MV_CLIENTS_CHATS'
        date = '2026-01-15'
        skill = 'gpt_mv_resolvers'
        
        query = f"""
        SELECT CONVERSATION_ID, TEXT, SENT_BY
        FROM {table}
        WHERE TO_DATE(START_DATE) = '{date}'
          AND SKILL ILIKE '%{skill}%'
        """
        
        assert 'SELECT' in query
        assert table in query
        assert date in query
        assert skill in query
    
    def test_build_insert_query(self):
        """Test building INSERT query."""
        table = 'LLM_EVAL.PUBLIC.TEST_RAW_DATA'
        
        query = f"""
        INSERT INTO {table} 
        (CONVERSATION_ID, DATE, DEPARTMENT, LLM_RESPONSE)
        VALUES (%s, %s, %s, %s)
        """
        
        assert 'INSERT' in query
        assert table in query
        assert '%s' in query  # Parameterized


class TestMetricsParsing:
    """Test metrics parsing from LLM responses."""
    
    def test_parse_ftr_response(self):
        """Test parsing FTR (First Time Resolution) response."""
        response = '{"chatResolution": "yes", "reason": "Issue resolved"}'
        parsed = json.loads(response)
        
        chat_resolution = parsed.get('chatResolution', '').lower()
        is_resolved = chat_resolution == 'yes'
        
        assert is_resolved == True
    
    def test_parse_false_promises_response(self):
        """Test parsing False Promises response."""
        response = '{"madePromise": "no", "details": "No promises made"}'
        parsed = json.loads(response)
        
        made_promise = parsed.get('madePromise', '').lower()
        has_promise = made_promise == 'yes'
        
        assert has_promise == False
    
    def test_parse_threatening_response(self):
        """Test parsing Threatening response."""
        response = '{"Result": "none", "Explanation": "No threats detected"}'
        parsed = json.loads(response)
        
        result = parsed.get('Result', '').lower()
        is_threatening = result not in ('none', 'n/a', '')
        
        assert is_threatening == False
    
    def test_calculate_percentage(self):
        """Test percentage calculation."""
        count = 3
        denominator = 10
        
        percentage = (count / denominator * 100) if denominator > 0 else 0
        
        assert percentage == 30.0
    
    def test_handle_zero_denominator(self):
        """Test handling zero denominator."""
        count = 5
        denominator = 0
        
        percentage = (count / denominator * 100) if denominator > 0 else 0
        
        assert percentage == 0
