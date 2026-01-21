#!/usr/bin/env python3
"""
LLM Analysis Pipeline - Unified Runner

This is the main entry point for the LLM analysis pipeline.
It works in all environments:
- Snowflake (via stored procedures)
- GitHub Actions (scheduled daily runs)
- Google Colab (interactive testing)
- Local development

Usage:
    # Run all departments for yesterday
    python run_pipeline.py
    
    # Run specific departments
    python run_pipeline.py --departments MV_Resolvers CC_Resolvers
    
    # Run for a specific date
    python run_pipeline.py --date 2026-01-12
    
    # Run specific prompts only
    python run_pipeline.py --prompts client_suspecting_ai ftr
    
    # Skip metrics calculation
    python run_pipeline.py --no-metrics
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# =============================================================================
# IMPORTS (Conditional based on environment)
# =============================================================================

from config import (
    Environment,
    detect_environment,
    get_environment_info,
    setup_environment,
    print_environment_summary,
    load_config_from_env,
    ExecutionConfig,
)

from snowflake_llm_config import (
    get_snowflake_llm_departments_config,
    get_snowflake_base_departments_config,
    get_prompt_config,
    get_metrics_configuration,
    is_metrics_calc_available,
)


# =============================================================================
# PIPELINE RUNNER CLASS
# =============================================================================

class LLMPipelineRunner:
    """
    Unified pipeline runner that works in all environments.
    
    This class encapsulates the entire LLM analysis workflow:
    1. Read conversations from Snowflake
    2. Convert to appropriate format (XML, segments, etc.)
    3. Run LLM analysis (parallel in external mode)
    4. Write results to raw data tables
    5. Calculate and write metrics to summary tables
    """
    
    def __init__(
        self,
        llm_client_class: Any,
        data_access_class: Any,
        config: Optional[ExecutionConfig] = None
    ):
        """
        Initialize the pipeline runner.
        
        Args:
            llm_client_class: LLM client class (AsyncHTTPClient or SnowflakeUDFClient)
            data_access_class: Data access class (ConnectorDataAccess or SnowparkDataAccess)
            config: Optional execution configuration
        """
        self.llm_client_class = llm_client_class
        self.data_access_class = data_access_class
        self.config = config or load_config_from_env()
        
        # Will be initialized during run
        self.llm_client = None
        self.data_access = None
        
        # Results tracking
        self.results = {}
        self.errors = []
    
    def initialize(
        self,
        snowflake_config: Optional[Dict] = None,
        openai_api_key: Optional[str] = None
    ) -> None:
        """
        Initialize clients and data access.
        
        Args:
            snowflake_config: Optional Snowflake connection config
            openai_api_key: Optional OpenAI API key
        """
        print("\n🔌 Initializing connections...")
        
        # Get config from environment if not provided
        if snowflake_config is None:
            snowflake_config = self.config.snowflake.to_dict()
        
        if openai_api_key is None:
            openai_api_key = self.config.openai.default_key
        
        # Initialize data access
        from data_access import DataAccessConfig, create_data_access, DataAccessMode
        
        data_config = DataAccessConfig(
            account=snowflake_config.get('account', ''),
            user=snowflake_config.get('user', ''),
            password=snowflake_config.get('password', ''),
            warehouse=snowflake_config.get('warehouse', 'LLMS_WH'),
            database=snowflake_config.get('database', 'LLM_EVAL'),
            schema=snowflake_config.get('schema', 'PUBLIC'),
            role=snowflake_config.get('role', 'LLM_ROLE'),
        )
        
        self.data_access = create_data_access(
            mode=DataAccessMode.CONNECTOR,
            config=data_config
        )
        print("   ✅ Data access initialized")
        
        # Initialize LLM client
        self.llm_client = self.llm_client_class(
            api_key=openai_api_key,
            batch_size=self.config.openai.batch_size,
            delay_between_batches=self.config.openai.delay_between_batches
        )
        print("   ✅ LLM client initialized")
        
        # Initialize N8N client if API key is available (for tool_eval)
        if self.config.n8n.is_valid():
            from llm_clients import set_n8n_client
            set_n8n_client(self.config.n8n.api_key)
            print("   ✅ N8N client initialized (tool_eval enabled)")
        else:
            print("   ℹ️  N8N client not configured (tool_eval will be skipped)")
            print("   💡 Set N8N_API_KEY environment variable to enable tool_eval")
    
    def run(
        self,
        departments: List[str] = None,
        target_date: str = None,
        prompts: List[str] = None,
        calculate_metrics: bool = True,
        test_limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Run the full pipeline.
        
        Args:
            departments: List of departments to process (default: from config)
            target_date: Target date string YYYY-MM-DD (default: yesterday)
            prompts: List of prompt types to run (default: all)
            calculate_metrics: Whether to calculate metrics after LLM analysis
            test_limit: Limit conversations per prompt (for testing)
        
        Returns:
            Dictionary with results per department
        """
        # Set defaults
        if departments is None:
            departments = self.config.departments
        
        if target_date is None:
            target_date = self.config.target_date or self._get_yesterday()
        
        if prompts is None or prompts == ['*']:
            prompts = None  # Run all prompts
        
        print("\n" + "=" * 60)
        print("🚀 STARTING LLM ANALYSIS PIPELINE")
        print("=" * 60)
        print(f"   Departments: {', '.join(departments)}")
        print(f"   Target Date: {target_date}")
        print(f"   Prompts: {'All' if prompts is None else ', '.join(prompts)}")
        print(f"   Calculate Metrics: {calculate_metrics}")
        if test_limit:
            print(f"   Test Limit: {test_limit} conversations per prompt")
        print("=" * 60)
        
        # Process each department
        for dept in departments:
            try:
                dept_result = self._process_department(
                    dept, target_date, prompts, test_limit
                )
                self.results[dept] = dept_result
            except Exception as e:
                error_msg = f"Error processing {dept}: {str(e)}"
                print(f"\n❌ {error_msg}")
                self.errors.append(error_msg)
                self.results[dept] = {'success': False, 'error': str(e)}
        
        # Calculate metrics if requested
        if calculate_metrics:
            self._calculate_all_metrics(departments, target_date)
        
        # Print summary
        self._print_summary()
        
        return self.results
    
    def _process_department(
        self,
        department: str,
        target_date: str,
        selected_prompts: Optional[List[str]],
        test_limit: Optional[int]
    ) -> Dict[str, Any]:
        """Process a single department."""
        print(f"\n{'=' * 60}")
        print(f"📊 Processing Department: {department}")
        print(f"{'=' * 60}")
        
        # Get department configuration
        dept_config = get_snowflake_base_departments_config().get(department)
        if not dept_config:
            raise ValueError(f"Unknown department: {department}")
        
        full_config = get_snowflake_llm_departments_config().get(department, {})
        prompts_config = full_config.get('llm_prompts', {})
        
        # Determine which prompts to run
        if selected_prompts:
            prompts_to_run = [p for p in selected_prompts if p in prompts_config]
        else:
            prompts_to_run = list(prompts_config.keys())
        
        # Separate tool_eval from regular prompts
        # tool_eval requires message-level processing (different from XML conversion)
        tool_eval_prompts = [p for p in prompts_to_run if p == 'tool_eval']
        regular_prompts = [p for p in prompts_to_run if p != 'tool_eval']
        
        print(f"   Source Table: {dept_config.get('table_name')}")
        print(f"   Regular prompts: {len(regular_prompts)}")
        if tool_eval_prompts:
            print(f"   Tool eval prompts: {len(tool_eval_prompts)}")
        
        # Read conversations
        conversations_df = self._read_conversations(dept_config, target_date, test_limit)
        if conversations_df.empty:
            print(f"   ⚠️ No conversations found for {target_date}")
            return {'success': True, 'conversations': 0, 'prompts': {}}
        
        print(f"   ✅ Found {len(conversations_df)} conversations")
        
        prompt_results = {}
        
        # Process regular prompts (XML-based)
        if regular_prompts:
            # Convert to XML format
            xml_df = self._convert_to_xml(conversations_df, department)
            
            # Process each regular prompt
            for prompt_type in regular_prompts:
                prompt_config = prompts_config.get(prompt_type, {})
                result = self._process_prompt(
                    xml_df, department, target_date, prompt_type, prompt_config
                )
                prompt_results[prompt_type] = result
        
        # Process tool_eval prompts (message-level)
        if tool_eval_prompts:
            for prompt_type in tool_eval_prompts:
                prompt_config = prompts_config.get(prompt_type, {})
                result = self._process_tool_eval_prompt(
                    conversations_df, department, target_date, prompt_type, prompt_config
                )
                prompt_results[prompt_type] = result
        
        return {
            'success': True,
            'conversations': len(conversations_df),
            'prompts': prompt_results
        }
    
    def _read_conversations(
        self,
        dept_config: Dict,
        target_date: str,
        test_limit: Optional[int]
    ) -> 'pd.DataFrame':
        """Read conversations from Snowflake."""
        import pandas as pd
        
        source_table = dept_config.get('table_name')
        bot_skills = dept_config.get('bot_skills', [])
        skill_filter = bot_skills[0] if bot_skills else ''
        
        print(f"\n   📖 Reading conversations...")
        print(f"      Table: {source_table}")
        print(f"      Date: {target_date}")
        print(f"      Skill: {skill_filter}")
        
        # Build query
        query = f"""
        SELECT 
            CONVERSATION_ID,
            MESSAGE_ID,
            SENT_BY,
            TEXT,
            MESSAGE_TYPE,
            MESSAGE_SENT_TIME,
            SKILL,
            START_DATE
        FROM {source_table}
        WHERE TO_DATE(START_DATE) = '{target_date}'
        """
        
        if skill_filter:
            query += f"\n          AND SKILL ILIKE '%{skill_filter}%'"
        
        query += "\n        ORDER BY CONVERSATION_ID, MESSAGE_SENT_TIME"
        
        df = self.data_access.execute_sql(query)
        
        # Apply test limit
        if test_limit and not df.empty:
            conv_ids = df['CONVERSATION_ID'].unique()[:test_limit]
            df = df[df['CONVERSATION_ID'].isin(conv_ids)]
        
        return df
    
    def _convert_to_xml(self, conversations_df: 'pd.DataFrame', department: str) -> 'pd.DataFrame':
        """Convert conversations to XML format."""
        import pandas as pd
        
        print(f"\n   🔄 Converting to XML format...")
        
        try:
            from snowflake_llm_xml_converter import convert_conversations_to_xml_dataframe
            xml_df = convert_conversations_to_xml_dataframe(conversations_df, department)
            print(f"      ✅ Converted {len(xml_df)} conversations")
            return xml_df
        except Exception as e:
            print(f"      ⚠️ XML conversion error: {e}")
            print(f"      Using fallback concatenation...")
            
            # Fallback: simple concatenation
            xml_df = conversations_df.groupby('CONVERSATION_ID').apply(
                lambda x: pd.Series({
                    'CONVERSATION_XML': ' ||| '.join(
                        f"{row['SENT_BY']}: {row['TEXT']}"
                        for _, row in x.iterrows()
                    ),
                    'MESSAGE_COUNT': len(x)
                })
            ).reset_index()
            
            return xml_df
    
    def _process_prompt(
        self,
        xml_df: 'pd.DataFrame',
        department: str,
        target_date: str,
        prompt_type: str,
        prompt_config: Dict
    ) -> Dict[str, Any]:
        """Process a single prompt for all conversations."""
        import json
        
        print(f"\n   📝 Processing: {prompt_type}")
        
        system_prompt = prompt_config.get('system_prompt', '')
        model = prompt_config.get('model', 'gpt-4o-mini')
        max_tokens = prompt_config.get('max_tokens', 2048)
        output_table = prompt_config.get('output_table', f'{prompt_type.upper()}_RAW_DATA')
        full_table_name = f"LLM_EVAL.PUBLIC.{output_table}"
        
        print(f"      Model: {model}")
        print(f"      Output: {full_table_name}")
        
        # Prepare items for LLM processing
        items = [
            {
                'conversation_id': row['CONVERSATION_ID'],
                'content': str(row['CONVERSATION_XML'])
            }
            for _, row in xml_df.iterrows()
        ]
        
        # Process with LLM client
        results = self.llm_client.process_all(
            items=items,
            system_prompt=system_prompt,
            model=model,
            temperature=0,
            max_tokens=min(max_tokens, 4096),
            verbose=True
        )
        
        # Count successes (handle both LLMResponse objects and dicts)
        from llm_clients.interface import LLMResponse
        successful = sum(1 for r in results if (r.success if isinstance(r, LLMResponse) else r.get('success')))
        print(f"      ✅ {successful}/{len(results)} successful")
        
        # Write results to Snowflake
        written, errors = self._write_results(
            results, department, target_date, full_table_name, prompt_type
        )
        
        return {
            'total': len(results),
            'successful': successful,
            'written': written,
            'errors': errors
        }
    
    def _write_results(
        self,
        results: List,
        department: str,
        target_date: str,
        table_name: str,
        prompt_type: str = None
    ) -> Tuple[int, int]:
        """Write LLM results to Snowflake."""
        import json
        from llm_clients.interface import LLMResponse
        
        written = 0
        errors = 0
        
        for result in results:
            try:
                # Handle both LLMResponse objects and dicts
                if isinstance(result, LLMResponse):
                    conv_id = result.conversation_id
                    response = result.response or ''
                    is_success = result.success
                    error_msg = str(result.error) if result.error else 'LLM call failed'
                    usage = result.usage  # This is a property that returns a dict
                else:
                    # Fallback for dict results
                    conv_id = result.get('conversation_id')
                    response = result.get('response', '')
                    is_success = result.get('success', True)
                    error_msg = result.get('error', 'LLM call failed')
                    usage = result.get('usage') or {}
                
                if not is_success:
                    response = error_msg
                
                # Insert using parameterized query
                success = self.data_access.insert_llm_result(
                    table=table_name,
                    conversation_id=conv_id,
                    date=target_date,
                    department=department,
                    llm_response=str(response),
                    prompt_type=prompt_type,
                    tokens_breakdown={
                        'prompt_tokens': usage.get('prompt_tokens', 0),
                        'completion_tokens': usage.get('completion_tokens', 0),
                        'total_tokens': usage.get('total_tokens', 0)
                    }
                )
                
                if success:
                    written += 1
                else:
                    errors += 1
                    
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"      ⚠️ Write error: {str(e)[:80]}")
        
        print(f"      💾 Written: {written}, Errors: {errors}")
        return written, errors
    
    def _process_tool_eval_prompt(
        self,
        conversations_df: 'pd.DataFrame',
        department: str,
        target_date: str,
        prompt_type: str,
        prompt_config: Dict
    ) -> Dict[str, Any]:
        """
        Process tool_eval prompt using message-level analysis.
        
        Unlike regular prompts that use conversation-level XML, tool_eval:
        1. Converts conversations to message segments (one per consumer message)
        2. Fetches CLIENT_ATTRIBUTES and HISTORY from N8N API
        3. Runs LLM for each message segment individually
        4. Writes to TOOL_EVAL_RAW_DATA with MESSAGE_ID
        
        Args:
            conversations_df: Raw conversation messages DataFrame
            department: Department name
            target_date: Target date
            prompt_type: Should be 'tool_eval'
            prompt_config: Prompt configuration
            
        Returns:
            Processing results dict
        """
        import json
        import pandas as pd
        
        print(f"\n   🔧 Processing tool_eval (message-level)...")
        
        # Check if N8N client is available
        from llm_clients import is_n8n_client_available, get_n8n_client
        
        if not is_n8n_client_available():
            print(f"      ⚠️ N8N client not configured. Skipping tool_eval.")
            print(f"      💡 Set N8N_API_KEY environment variable to enable.")
            return {
                'total': 0,
                'successful': 0,
                'written': 0,
                'errors': 0,
                'skipped': True,
                'reason': 'N8N client not configured'
            }
        
        # Step 1: Convert to message segments
        print(f"      📝 Converting to message segments...")
        try:
            from snowflake_llm_message_converter import convert_conversations_to_message_segments
            segments_df = convert_conversations_to_message_segments(conversations_df, department)
            
            if segments_df is None or segments_df.empty:
                print(f"      ⚠️ No message segments generated")
                return {
                    'total': 0,
                    'successful': 0,
                    'written': 0,
                    'errors': 0,
                    'skipped': True,
                    'reason': 'No message segments'
                }
            
            print(f"      ✅ Generated {len(segments_df)} message segments")
            
        except ImportError as e:
            print(f"      ❌ Message converter not available: {e}")
            return {
                'total': 0,
                'successful': 0,
                'written': 0,
                'errors': 0,
                'skipped': True,
                'reason': f'Import error: {e}'
            }
        except Exception as e:
            print(f"      ❌ Conversion error: {e}")
            return {
                'total': 0,
                'successful': 0,
                'written': 0,
                'errors': 0,
                'skipped': True,
                'reason': f'Conversion error: {e}'
            }
        
        # Step 2: Fetch N8N data for each execution_id
        print(f"      📡 Fetching N8N data...")
        n8n_client = get_n8n_client()
        execution_ids = segments_df['EXECUTION_ID'].dropna().unique().tolist()
        
        if execution_ids:
            n8n_data = n8n_client.fetch_all(execution_ids, verbose=True)
            
            # Map N8N data back to segments
            def get_n8n_attrs(exec_id):
                if not exec_id or pd.isna(exec_id):
                    return json.dumps({})
                exec_id_str = str(int(float(exec_id))) if exec_id else ''
                data = n8n_data.get(exec_id_str)
                if data and data.success:
                    return json.dumps(data.client_attributes)
                return json.dumps({})
            
            def get_n8n_history(exec_id):
                if not exec_id or pd.isna(exec_id):
                    return json.dumps([])
                exec_id_str = str(int(float(exec_id))) if exec_id else ''
                data = n8n_data.get(exec_id_str)
                if data and data.success:
                    return json.dumps(data.history)
                return json.dumps([])
            
            segments_df['CLIENT_ATTRIBUTES'] = segments_df['EXECUTION_ID'].apply(get_n8n_attrs)
            segments_df['HISTORY'] = segments_df['EXECUTION_ID'].apply(get_n8n_history)
        else:
            segments_df['CLIENT_ATTRIBUTES'] = json.dumps({})
            segments_df['HISTORY'] = json.dumps([])
        
        # Step 3: Prepare LLM calls
        system_prompt = prompt_config.get('system_prompt', '')
        model = prompt_config.get('model', 'gpt-5')
        max_tokens = prompt_config.get('max_tokens', 30000)
        output_table = prompt_config.get('output_table', 'TOOL_EVAL_RAW_DATA')
        full_table_name = f"LLM_EVAL.PUBLIC.{output_table}"
        
        print(f"      Model: {model}")
        print(f"      Output: {full_table_name}")
        
        # Build items for LLM - each message gets individual LLM call
        items = []
        for _, row in segments_df.iterrows():
            # Build content with message, client attributes, and history
            message_content = str(row.get('MESSAGE', ''))
            
            items.append({
                'conversation_id': f"{row['CONVERSATION_ID']}_{row['MESSAGE_ID']}",
                'content': message_content,
                'segment_data': row.to_dict()  # Keep full row for writing
            })
        
        # Step 4: Process with LLM
        print(f"      🤖 Running LLM analysis for {len(items)} messages...")
        
        results = self.llm_client.process_all(
            items=[{'conversation_id': i['conversation_id'], 'content': i['content']} for i in items],
            system_prompt=system_prompt,
            model=model,
            temperature=0,
            max_tokens=min(max_tokens, 30000),
            verbose=True
        )
        
        # Count successes (handle both LLMResponse objects and dicts)
        from llm_clients.interface import LLMResponse
        successful = sum(1 for r in results if (r.success if isinstance(r, LLMResponse) else r.get('success')))
        print(f"      ✅ {successful}/{len(results)} LLM calls successful")
        
        # Step 5: Write results to TOOL_EVAL_RAW_DATA
        written = 0
        errors = 0
        
        for i, result in enumerate(results):
            try:
                segment_data = items[i]['segment_data']
                conv_id = segment_data.get('CONVERSATION_ID', '')
                message_id = segment_data.get('MESSAGE_ID', '')
                message = segment_data.get('MESSAGE', '')
                client_attrs = segment_data.get('CLIENT_ATTRIBUTES', '{}')
                history = segment_data.get('HISTORY', '[]')
                actual_tools = segment_data.get('ACTUAL_TOOLS_CALLED', '[]')
                execution_id = str(segment_data.get('EXECUTION_ID', '')) if segment_data.get('EXECUTION_ID') else ''
                target_skill = segment_data.get('TARGET_SKILL', '')
                customer_name = segment_data.get('CUSTOMER_NAME', '')
                
                # Handle both LLMResponse objects and dicts
                if isinstance(result, LLMResponse):
                    llm_response = result.response or '' if result.success else (str(result.error) if result.error else 'LLM call failed')
                else:
                    llm_response = result.get('response', '') if result.get('success') else result.get('error', 'LLM call failed')
                
                success = self.data_access.insert_tool_eval_result(
                    table=full_table_name,
                    conversation_id=conv_id,
                    message_id=message_id,
                    date=target_date,
                    department=department,
                    message=message,
                    client_attributes=client_attrs,
                    history=history,
                    actual_tools_called=actual_tools,
                    llm_response=str(llm_response),
                    execution_id=execution_id,
                    target_skill=target_skill,
                    customer_name=customer_name
                )
                
                if success:
                    written += 1
                else:
                    errors += 1
                    
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"      ⚠️ Write error: {str(e)[:80]}")
        
        print(f"      💾 Tool eval written: {written}, Errors: {errors}")
        
        return {
            'total': len(results),
            'successful': successful,
            'written': written,
            'errors': errors,
            'message_segments': len(segments_df)
        }
    
    def _calculate_all_metrics(self, departments: List[str], target_date: str) -> None:
        """
        Calculate metrics for all departments and write to summary tables.
        
        Uses the same pattern as update_department_master_summary() in 
        snowflake_llm_processor.py - directly calls metric functions from
        snowflake_llm_config.get_metrics_configuration().
        """
        print(f"\n{'=' * 60}")
        print("📊 CALCULATING METRICS")
        print(f"{'=' * 60}")
        
        if not is_metrics_calc_available():
            print("   ⚠️ Metrics calculation functions not loaded")
            print("   💡 Ensure snowflake_llm_metrics_calc.py is importable")
            return
        
        # Get metrics configuration - same as production
        metrics_config = get_metrics_configuration()
        
        for dept in departments:
            print(f"\n   📈 {dept}:")
            
            dept_config = metrics_config.get(dept)
            if not dept_config:
                print(f"      ⚠️ No metrics configuration for {dept}")
                continue
            
            master_table = dept_config.get('master_table')
            metrics_to_run = dept_config.get('metrics', {})
            
            if not metrics_to_run:
                print(f"      ⚠️ No metrics defined for {dept}")
                continue
            
            print(f"      Master Table: LLM_EVAL.PUBLIC.{master_table}")
            print(f"      Metrics to calculate: {len(metrics_to_run)}")
            
            # Sort metrics by order
            sorted_metrics = sorted(
                metrics_to_run.items(),
                key=lambda x: x[1].get('order', 999)
            )
            
            metric_results = {}
            successful_metrics = 0
            failed_metrics = 0
            
            for metric_name, metric_config in sorted_metrics:
                try:
                    # Get the function - can be callable or string
                    calc_function = metric_config.get('function')
                    
                    if isinstance(calc_function, str):
                        # Lookup by name from snowflake_llm_metrics_calc
                        import snowflake_llm_metrics_calc as metrics_module
                        calc_function = getattr(metrics_module, calc_function, None)
                    
                    if not callable(calc_function):
                        print(f"      ❌ {metric_name}: Function not callable")
                        for col in metric_config.get('columns', []):
                            metric_results[col] = None
                        failed_metrics += 1
                        continue
                    
                    # Call the metric function (same signature as production)
                    # All functions take: session, department_name, target_date
                    result = calc_function(self.data_access, dept, target_date)
                    
                    # Map results to column names
                    columns = metric_config.get('columns', [])
                    if isinstance(result, tuple):
                        success_flag = result[0]
                        if success_flag and len(result) > 1:
                            stats = result[1]
                            if isinstance(stats, dict):
                                for col in columns:
                                    # Try multiple key formats
                                    for key in [col, col.lower(), col.upper()]:
                                        if key in stats:
                                            metric_results[col] = stats[key]
                                            break
                                    else:
                                        metric_results[col] = None
                                
                                # Print result summary
                                count = stats.get('count', stats.get('COUNT', '?'))
                                denom = stats.get('denominator', stats.get('DENOMINATOR', stats.get('total', '?')))
                                pct = stats.get('percentage', stats.get('PERCENTAGE', 0))
                                print(f"      ✅ {metric_name}: {count}/{denom} ({pct:.1f}%)")
                                successful_metrics += 1
                            else:
                                print(f"      ⚠️ {metric_name}: Unexpected result format")
                                failed_metrics += 1
                        else:
                            error_msg = result[1].get('error', 'Unknown') if len(result) > 1 and isinstance(result[1], dict) else 'Failed'
                            print(f"      ❌ {metric_name}: {error_msg}")
                            failed_metrics += 1
                    else:
                        print(f"      ⚠️ {metric_name}: Unexpected return type {type(result)}")
                        failed_metrics += 1
                        
                except Exception as e:
                    print(f"      ❌ {metric_name}: {type(e).__name__}: {str(e)[:60]}")
                    for col in metric_config.get('columns', []):
                        metric_results[col] = None
                    failed_metrics += 1
            
            # Write to summary table
            if metric_results and master_table:
                try:
                    self._write_summary_row(
                        f"LLM_EVAL.PUBLIC.{master_table}",
                        dept, target_date, metric_results
                    )
                    print(f"      💾 Wrote {len(metric_results)} columns to {master_table}")
                except Exception as e:
                    print(f"      ❌ Error writing summary: {str(e)[:60]}")
            
            print(f"      📋 Summary: {successful_metrics} succeeded, {failed_metrics} failed")
    
    def _write_summary_row(
        self, 
        table_name: str, 
        department: str, 
        target_date: str, 
        columns: Dict[str, Any]
    ) -> None:
        """Write metrics results to summary table using SQL."""
        # Delete existing row for this date/department
        delete_sql = f"""
        DELETE FROM {table_name}
        WHERE DATE = '{target_date}' AND DEPARTMENT = '{department}'
        """
        self.data_access.execute_sql_no_return(delete_sql)
        
        # Build insert with all columns
        all_columns = {'DATE': target_date, 'DEPARTMENT': department}
        all_columns.update(columns)
        
        col_names = []
        col_values = []
        
        for col, val in all_columns.items():
            col_names.append(col)
            if val is None:
                col_values.append("NULL")
            elif isinstance(val, str):
                escaped = val.replace("'", "''")
                col_values.append(f"'{escaped}'")
            elif isinstance(val, bool):
                col_values.append("TRUE" if val else "FALSE")
            else:
                col_values.append(str(val))
        
        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(col_names)})
        VALUES ({', '.join(col_values)})
        """
        self.data_access.execute_sql_no_return(insert_sql)
    
    def _print_summary(self) -> None:
        """Print pipeline execution summary."""
        print(f"\n{'=' * 60}")
        print("📋 PIPELINE SUMMARY")
        print(f"{'=' * 60}")
        
        total_convs = 0
        total_success = 0
        total_prompts = 0
        
        for dept, result in self.results.items():
            if result.get('success'):
                convs = result.get('conversations', 0)
                prompts = result.get('prompts', {})
                
                dept_success = sum(p.get('successful', 0) for p in prompts.values())
                dept_total = sum(p.get('total', 0) for p in prompts.values())
                
                print(f"\n   {dept}:")
                print(f"      Conversations: {convs}")
                print(f"      Prompts Run: {len(prompts)}")
                print(f"      LLM Calls: {dept_success}/{dept_total}")
                
                total_convs += convs
                total_success += dept_success
                total_prompts += len(prompts)
            else:
                print(f"\n   {dept}: ❌ Failed - {result.get('error', 'Unknown error')}")
        
        print(f"\n   {'─' * 40}")
        print(f"   Total Conversations: {total_convs}")
        print(f"   Total Prompts: {total_prompts}")
        print(f"   Total Successful LLM Calls: {total_success}")
        
        if self.errors:
            print(f"\n   ⚠️ Errors: {len(self.errors)}")
            for err in self.errors[:5]:
                print(f"      - {err[:60]}")
        
        print(f"\n{'=' * 60}")
    
    def _get_yesterday(self) -> str:
        """Get yesterday's date as string."""
        yesterday = datetime.now() - timedelta(days=1)
        return yesterday.strftime('%Y-%m-%d')
    
    def close(self) -> None:
        """Clean up resources."""
        if self.data_access:
            self.data_access.close()


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main(
    departments: List[str] = None,
    target_date: str = None,
    prompts: List[str] = None,
    calculate_metrics: bool = True,
    test_limit: Optional[int] = None,
    snowflake_config: Optional[Dict] = None,
    openai_api_key: Optional[str] = None,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Main entry point for LLM analysis pipeline.
    
    Works in Snowflake, GitHub Actions, Colab, or local.
    
    Args:
        departments: List of departments to process
        target_date: Target date (YYYY-MM-DD format)
        prompts: List of prompt types to run (None = all)
        calculate_metrics: Whether to calculate metrics
        test_limit: Limit conversations per prompt (for testing)
        snowflake_config: Optional Snowflake connection config
        openai_api_key: Optional OpenAI API key
        dry_run: If True, validate configuration without processing
    
    Returns:
        Dictionary with results per department
    """
    # 1. Detect and setup environment
    print("=" * 60)
    print("🌍 LLM ANALYSIS PIPELINE" + (" [DRY RUN]" if dry_run else ""))
    print("=" * 60)
    
    env_info = get_environment_info()
    print(f"   Environment: {env_info.environment.value}")
    print(f"   Async Support: {'✅' if env_info.can_run_async else '❌'}")
    
    # Handle dry run mode
    if dry_run:
        print("\n🔍 DRY RUN MODE - Validating configuration...")
        print(f"   Departments: {departments or 'all'}")
        print(f"   Target Date: {target_date or 'yesterday'}")
        print(f"   Prompts: {prompts or 'all'}")
        print(f"   Calculate Metrics: {calculate_metrics}")
        print(f"   Test Limit: {test_limit or 'unlimited'}")
        print("\n   ✅ Configuration validated - no data processing performed")
        return {'dry_run': True, 'status': 'validated'}
    
    # 2. Setup environment-specific clients
    try:
        llm_client_class, data_access_class = setup_environment(env_info.environment)
    except Exception as e:
        print(f"❌ Environment setup failed: {e}")
        return {'error': str(e)}
    
    # 3. Create and initialize runner
    runner = LLMPipelineRunner(llm_client_class, data_access_class)
    
    try:
        runner.initialize(snowflake_config, openai_api_key)
        
        # 4. Run pipeline
        results = runner.run(
            departments=departments,
            target_date=target_date,
            prompts=prompts,
            calculate_metrics=calculate_metrics,
            test_limit=test_limit
        )
        
        return results
        
    finally:
        runner.close()


# =============================================================================
# CLI INTERFACE
# =============================================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='LLM Analysis Pipeline - Unified Runner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all departments for yesterday
  python run_pipeline.py
  
  # Run specific departments
  python run_pipeline.py --departments MV_Resolvers CC_Resolvers
  
  # Run for a specific date
  python run_pipeline.py --date 2026-01-12
  
  # Test with limited conversations
  python run_pipeline.py --test-limit 10
  
  # Skip metrics calculation
  python run_pipeline.py --no-metrics
        """
    )
    
    parser.add_argument(
        '--departments', '-d',
        nargs='+',
        default=None,
        help='Departments to process (default: from config or MV_Resolvers)'
    )
    
    parser.add_argument(
        '--date',
        default=None,
        help='Target date YYYY-MM-DD (default: yesterday)'
    )
    
    parser.add_argument(
        '--prompts', '-p',
        nargs='+',
        default=None,
        help='Specific prompts to run (default: all)'
    )
    
    parser.add_argument(
        '--no-metrics',
        action='store_true',
        help='Skip metrics calculation'
    )
    
    parser.add_argument(
        '--test-limit',
        type=int,
        default=None,
        help='Limit conversations per prompt (for testing)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry run mode - validate without writing to Snowflake'
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    results = main(
        departments=args.departments,
        target_date=args.date,
        prompts=args.prompts,
        calculate_metrics=not args.no_metrics,
        test_limit=args.test_limit,
        dry_run=args.dry_run
    )
    
    # Exit with appropriate code
    # Handle dry_run mode which returns {'dry_run': True, 'status': 'validated'}
    if results.get('dry_run'):
        print("\n✅ Dry run completed successfully")
        sys.exit(0)
    
    # Handle error case
    if results.get('error'):
        print(f"\n❌ Pipeline failed: {results.get('error')}")
        sys.exit(1)
    
    # Check department results - each value should be a dict with 'success' key
    has_failure = False
    for dept, result in results.items():
        if isinstance(result, dict) and not result.get('success', True):
            has_failure = True
            break
    
    if has_failure:
        sys.exit(1)
    sys.exit(0)
