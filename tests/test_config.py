"""
Tests for configuration loading and department config.
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestDepartmentConfig:
    """Test department configuration loading."""
    
    def test_get_base_departments_config(self):
        """Test that base department config loads correctly."""
        from snowflake_llm_config import get_snowflake_base_departments_config
        
        config = get_snowflake_base_departments_config()
        
        # Check that config is a dict
        assert isinstance(config, dict)
        
        # Check expected departments exist
        expected_departments = ['MV_Resolvers', 'CC_Resolvers', 'CC_Delighters', 'Doctors']
        for dept in expected_departments:
            assert dept in config, f"Department {dept} not found in config"
    
    def test_department_has_required_fields(self):
        """Test that each department has required configuration fields."""
        from snowflake_llm_config import get_snowflake_base_departments_config
        
        config = get_snowflake_base_departments_config()
        required_fields = ['bot_skills', 'table_name']
        
        for dept_name, dept_config in config.items():
            for field in required_fields:
                assert field in dept_config, f"Department {dept_name} missing required field: {field}"
    
    def test_mv_resolvers_config(self):
        """Test MV_Resolvers specific configuration."""
        from snowflake_llm_config import get_snowflake_base_departments_config
        
        config = get_snowflake_base_departments_config()
        mv_config = config.get('MV_Resolvers', {})
        
        assert 'GPT_MV_RESOLVERS' in mv_config.get('bot_skills', [])
        assert 'MV_CLIENTS_CHATS' in mv_config.get('table_name', '')
    
    def test_cc_resolvers_config(self):
        """Test CC_Resolvers specific configuration."""
        from snowflake_llm_config import get_snowflake_base_departments_config
        
        config = get_snowflake_base_departments_config()
        cc_config = config.get('CC_Resolvers', {})
        
        assert 'GPT_CC_RESOLVERS' in cc_config.get('bot_skills', [])
        assert 'CC_CLIENT_CHATS' in cc_config.get('table_name', '')


class TestLLMPromptsConfig:
    """Test LLM prompts configuration."""
    
    def test_get_llm_departments_config(self):
        """Test that LLM departments config loads correctly."""
        from snowflake_llm_config import get_snowflake_llm_departments_config
        
        config = get_snowflake_llm_departments_config()
        
        assert isinstance(config, dict)
        assert 'MV_Resolvers' in config
    
    def test_mv_resolvers_has_prompts(self):
        """Test that MV_Resolvers has LLM prompts configured."""
        from snowflake_llm_config import get_snowflake_llm_departments_config
        
        config = get_snowflake_llm_departments_config()
        mv_config = config.get('MV_Resolvers', {})
        
        assert 'llm_prompts' in mv_config
        prompts = mv_config['llm_prompts']
        
        # Check some expected prompts exist
        expected_prompts = ['client_suspecting_ai', 'false_promises', 'ftr']
        for prompt in expected_prompts:
            assert prompt in prompts, f"Expected prompt {prompt} not found in MV_Resolvers"
    
    def test_prompt_has_required_fields(self):
        """Test that prompts have required fields."""
        from snowflake_llm_config import get_snowflake_llm_departments_config
        
        config = get_snowflake_llm_departments_config()
        mv_prompts = config.get('MV_Resolvers', {}).get('llm_prompts', {})
        
        required_fields = ['system_prompt', 'model', 'output_table']
        
        for prompt_name, prompt_config in mv_prompts.items():
            if prompt_name == 'test':  # Skip test prompts
                continue
            for field in required_fields:
                assert field in prompt_config, f"Prompt {prompt_name} missing field: {field}"
    
    def test_get_prompt_config(self):
        """Test getting specific prompt configuration."""
        from snowflake_llm_config import get_prompt_config
        
        config = get_prompt_config('MV_Resolvers', 'client_suspecting_ai')
        
        assert config is not None
        assert 'system_prompt' in config
        assert 'model' in config


class TestDepartmentPromptTypes:
    """Test getting prompt types for departments."""
    
    def test_get_department_prompt_types(self):
        """Test getting all prompt types for a department."""
        from snowflake_llm_config import get_department_prompt_types
        
        prompt_types = get_department_prompt_types('MV_Resolvers')
        
        assert isinstance(prompt_types, list)
        assert len(prompt_types) > 0
        assert 'client_suspecting_ai' in prompt_types
    
    def test_unknown_department_returns_empty(self):
        """Test that unknown department returns empty list."""
        from snowflake_llm_config import get_department_prompt_types
        
        prompt_types = get_department_prompt_types('NonExistentDepartment')
        
        assert isinstance(prompt_types, list)
        assert len(prompt_types) == 0
