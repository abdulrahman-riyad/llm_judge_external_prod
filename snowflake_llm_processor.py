"""
LLM Processing Module for Snowflake Chat Analysis
Handles LLM analysis calls and processing logic

Supports multiple execution modes:
- 'synchronous': Traditional Snowflake UDF calls (default)
- 'batch_submit': OpenAI Batch API via Snowflake
- 'external': External HTTP calls via llm_clients module (for GitHub Actions/Colab)
"""

import snowflake.snowpark as snowpark
import pandas as pd
from datetime import datetime
import json
import traceback
import os
from snowflake_llm_config import get_snowflake_llm_departments_config, get_prompt_config, get_metrics_configuration, get_department_summary_schema, get_snowflake_base_departments_config
from snowflake_llm_xml_converter import convert_conversations_to_xml_dataframe, validate_xml_conversion
from snowflake_phase2_core_analytics import process_department_phase1, process_department_phase1_multi_day, process_department_phase1_cw
from snowflake_llm_metrics_calc import *

# =============================================================================
# LLM CLIENT INTEGRATION (for external execution mode)
# =============================================================================
# Try to import the new llm_clients module for external execution support.
# Falls back gracefully if not available (e.g., when running inside Snowflake).

_LLM_CLIENT = None
_LLM_CLIENTS_AVAILABLE = False

try:
    from llm_clients import (
        create_llm_client,
        detect_execution_environment,
        ExecutionMode as LLMExecutionMode,
        LLMClientInterface
    )
    _LLM_CLIENTS_AVAILABLE = True
except ImportError:
    # llm_clients not available (running inside Snowflake or not installed)
    _LLM_CLIENTS_AVAILABLE = False


def get_llm_client(config: dict = None) -> 'LLMClientInterface':
    """
    Get or create LLM client for external execution mode.
    
    Args:
        config: Optional config dict with 'api_key', 'batch_size', etc.
    
    Returns:
        LLMClientInterface instance
    
    Raises:
        RuntimeError: If llm_clients module is not available
    """
    global _LLM_CLIENT
    
    if not _LLM_CLIENTS_AVAILABLE:
        raise RuntimeError(
            "llm_clients module not available. "
            "External execution mode requires the llm_clients package. "
            "Install it or use 'synchronous' mode instead."
        )
    
    if _LLM_CLIENT is None:
        if config is None:
            config = {}
        # Auto-detect API key from environment if not provided
        if 'api_key' not in config and os.getenv('OPENAI_API_KEY'):
            config['api_key'] = os.getenv('OPENAI_API_KEY')
        _LLM_CLIENT = create_llm_client(
            mode=LLMExecutionMode.EXTERNAL_HTTP,
            config=config
        )
    
    return _LLM_CLIENT


def set_llm_client(client: 'LLMClientInterface') -> None:
    """Set the LLM client for external execution mode."""
    global _LLM_CLIENT
    _LLM_CLIENT = client


def is_external_execution_available() -> bool:
    """Check if external execution mode is available."""
    return _LLM_CLIENTS_AVAILABLE


def _exclude_agent_and_after_agent_intervention(messages_df: pd.DataFrame) -> pd.DataFrame:
    """
    Exclude human-agent messages (SENT_BY == 'AGENT') and drop ALL messages at/after the first agent message per conversation.

    This is a token/cost and correctness safeguard for prompts that must ignore agent content and stop evaluation once a human agent intervenes.
    """
    if messages_df is None or messages_df.empty:
        return messages_df

    required_cols = {'CONVERSATION_ID', 'MESSAGE_SENT_TIME', 'SENT_BY'}
    if not required_cols.issubset(set(messages_df.columns)):
        return messages_df

    df = messages_df.copy()
    df['_SENT_BY_UPPER'] = df['SENT_BY'].fillna('').astype(str).str.upper()

    # Find first agent message timestamp per conversation (if any)
    agent_min_time = (
        df[df['_SENT_BY_UPPER'] == 'AGENT']
        .groupby('CONVERSATION_ID', dropna=False)['MESSAGE_SENT_TIME']
        .min()
        .rename('_FIRST_AGENT_TIME')
    )

    if not agent_min_time.empty:
        df = df.merge(agent_min_time, left_on='CONVERSATION_ID', right_index=True, how='left')
        df = df[(df['_FIRST_AGENT_TIME'].isna()) | (df['MESSAGE_SENT_TIME'] < df['_FIRST_AGENT_TIME'])]

    # Always remove agent messages themselves
    df = df[df['_SENT_BY_UPPER'] != 'AGENT']

    drop_cols = [c for c in ['_SENT_BY_UPPER', '_FIRST_AGENT_TIME'] if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    return df


def get_table_columns(session: snowpark.Session, table_name: str) -> list:
    try:
        cols = session.sql(f"SHOW COLUMNS IN {table_name}").collect()
        return [c['column_name'] for c in cols]
    except Exception:
        return []


def should_use_full_insert(session: snowpark.Session, table_name: str, dynamic_columns: list) -> bool:
    existing_cols = [c.upper() for c in get_table_columns(session, table_name)]
    if not existing_cols:
        # Table not found → full insert (creator will happen in insert_raw_data_with_cleanup)
        return True
    target_cols = [c.upper() for c in dynamic_columns]
    # If exact set match (ignoring order), we can use full insert cleanup (which deletes and reinserts)
    return set(target_cols) == set([c for c in existing_cols if c not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']])


def summary_row_exists(session: snowpark.Session, table_name: str, department: str, target_date: str) -> bool:
    try:
        q = f"SELECT 1 FROM {table_name} WHERE DATE='{target_date}' AND DEPARTMENT='{department}' LIMIT 1"
        return len(session.sql(q).collect()) > 0
    except Exception:
        return False


def insert_raw_data_partial(session: snowpark.Session, table_name: str, department: str, target_date: str, values_dict: dict) -> bool:
    # Ensure table exists; if not, create with full schema from config
    try:
        cols_in_table = get_table_columns(session, table_name)
        if not cols_in_table:
            # Create table with all metric columns from config
            schema_cols = get_department_summary_schema(department)
            create_cols_str = ",\n    ".join([f"{k} {v}" for k, v in schema_cols.items()])
            session.sql(f"CREATE TABLE {table_name} (\n    {create_cols_str}\n)").collect()
            cols_in_table = list(schema_cols.keys())
    except Exception as e:
        print(f"   ❌ Failed ensuring table {table_name}: {str(e)}")
        return False

    # Build lists for numeric vs summary columns
    numeric_cols = []
    summary_cols = []
    for col in cols_in_table:
        if col.upper() in ['DATE', 'DEPARTMENT', 'TIMESTAMP']:
            continue
        # Heuristic: treat columns ending with _ANALYSIS_SUMMARY as summary JSON text columns
        if col.upper().endswith('ANALYSIS_SUMMARY'):
            summary_cols.append(col)
        else:
            numeric_cols.append(col)

    if summary_row_exists(session, table_name, department, target_date):
        # UPDATE only provided columns
        set_parts = ["TIMESTAMP = CURRENT_TIMESTAMP()"]
        for k, v in values_dict.items():
            if k in ['DATE', 'DEPARTMENT', 'TIMESTAMP']:
                continue
            if k not in cols_in_table:
                continue
            if v is None:
                set_parts.append(f"{k} = NULL")
            else:
                # Quote strings; others as-is
                if isinstance(v, str):
                    safe_v = v.replace("'", "''")
                    set_parts.append(f"{k} = '{safe_v}'")
                else:
                    set_parts.append(f"{k} = {v}")
        set_clause = ", ".join(set_parts)
        sql = f"UPDATE {table_name} SET {set_clause} WHERE DATE='{target_date}' AND DEPARTMENT='{department}'"
        session.sql(sql).collect()
        return True
    else:
        # INSERT one row: provided values; all other numeric cols = 0.0; all summary cols = default JSON with zeros
        row = {c: None for c in cols_in_table}
        row['DATE'] = target_date
        row['DEPARTMENT'] = department
        # TIMESTAMP handled by default CURRENT_TIMESTAMP in insert using explicit column
        for k, v in values_dict.items():
            if k in row:
                row[k] = v
        for c in numeric_cols:
            if row.get(c) is None:
                row[c] = 0.0
        for c in summary_cols:
            if row.get(c) is None:
                # default simple JSON with zeros
                row[c] = '{"chats_analyzed": 0, "chats_parsed": 0, "chats_failed": 0, "failure_percentage": 0.0}'

        # Build INSERT
        cols = ['DATE', 'DEPARTMENT', 'TIMESTAMP'] + [c for c in cols_in_table if c not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
        values_sql = []
        for c in cols:
            v = row.get(c)
            if c == 'TIMESTAMP':
                values_sql.append('CURRENT_TIMESTAMP()')
            elif v is None:
                values_sql.append('NULL')
            elif isinstance(v, str):
                values_sql.append("'" + v.replace("'", "''") + "'")
            else:
                values_sql.append(str(v))
        sql = f"INSERT INTO {table_name} (" + ", ".join(cols) + ") VALUES (" + ", ".join(values_sql) + ")"
        session.sql(sql).collect()
        return True

def format_error_details(e, context=""):
    """
    Format exception details for comprehensive error reporting.
    
    Args:
        e: Exception object
        context: Additional context about where the error occurred
    
    Returns:
        Formatted error string with full details
    """
    error_details = traceback.format_exc()
    return f"""
{'=' * 50}
🚨 LLM ERROR DETAILS {f"- {context}" if context else ""}
{'=' * 50}
Error Type: {type(e).__name__}
Error Message: {str(e)}

Full Traceback:
{error_details}
{'=' * 50}
"""


def run_snowflake_llm_analysis(session: snowpark.Session, xml_content, prompt_config, department_name):
    """
    Run LLM analysis using either OpenAI or Gemini based on model_type preference
    
    Args:
        session: Snowflake session
        xml_content: XML formatted conversation content
        prompt_config: Prompt configuration dictionary
    
    Returns:
        LLM response string or None if failed
    """
    try:
        # Escape single quotes in content for SQL
        escaped_content = xml_content.replace("'", "''")
        escaped_prompt = prompt_config['prompt'].replace("'", "''")
        escaped_system = prompt_config['system_prompt'].replace("'", "''")
        
        # Get model configuration
        model_type = prompt_config.get('model_type', 'openai').lower()
        model = prompt_config.get('model', 'gpt-4o-mini')
        temperature = prompt_config.get('temperature', 0.2)
        max_tokens = prompt_config.get('max_tokens', 2048)
        
        # Extract response_format from config
        response_format = prompt_config.get('response_format', None)
        response_format_param = f"'{response_format}'" if response_format else 'NULL'
        
        # Wrap message for OpenAI when using JSON format (exclude tool_eval)
        prompt_type_extracted = prompt_config.get('output_table', '').replace('_RAW_DATA', '').lower()
        print(f"    🔍 JSON wrapping check (synchronous): response_format={response_format}, model_type={model_type}, prompt_type={prompt_type_extracted}, output_table={prompt_config.get('output_table', 'N/A')}")
        if response_format == 'json_object' and model_type == 'openai' and prompt_type_extracted != 'tool_eval':
            print(f"    ✅ Adding JSON prefix to escaped_content")
            escaped_content = f"Return your response as JSON.\\n\\n{escaped_content}"
        else:
            print(f"    ⏭️  Skipping JSON prefix")
        
        # Choose the appropriate LLM function based on model_type
        if model_type == 'openai':
            llm_function = 'openai_chat_system'
        elif model_type == 'gemini':
            llm_function = 'gemini_chat_system'
        else:
            raise ValueError(f"Unsupported model_type: {model_type}")
        
        # Build SQL query with the appropriate function
        sql_query = f"""
        SELECT {llm_function}(
            '{escaped_content}',
            '{escaped_system}',
            '{model}',
            {temperature},
            {max_tokens},
            '{prompt_config.get('reasoning_effort', 'minimal')}',
            '{department_name}',
            'single',
            NULL,
            NULL,
            NULL,
            {response_format_param}
        ) AS llm_response
        """
        
        result = session.sql(sql_query).collect()
        return result[0]['LLM_RESPONSE'] if result else None
        
    except Exception as e:
        print(f"    ⚠️  LLM call failed ({model_type}/{model}): {str(e)}")
        return None


def clean_dataframe_for_snowflake(df):
    """
    Clean DataFrame to ensure Snowflake/PyArrow compatibility.
    
    Args:
        df: Input DataFrame
    
    Returns:
        Cleaned DataFrame with consistent data types
    """
    df_clean = df.copy()
    
    # Convert all object columns to string to avoid mixed type issues
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            # Convert to string and handle NaN
            df_clean[col] = df_clean[col].astype(str).replace('nan', '')
    
    # Ensure datetime columns are properly typed
    datetime_columns = ['ANALYSIS_DATE']
    for col in datetime_columns:
        if col in df_clean.columns:
            try:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
            except:
                pass
    
    return df_clean


def insert_raw_data_with_cleanup(session: snowpark.Session, table_name: str, department: str, target_date, dataframe: pd.DataFrame, columns: list, additional_filter: dict = None):
    """
    Dynamically insert raw data into a table with date-based cleanup.
    
    Args:
        session: Snowflake session object
        table_name: Name of the target table
        department: Department value to add to all rows
        target_date: Target date for the data
        dataframe: Pandas dataframe containing the data to insert
        columns: List of column names that should match dataframe columns
        additional_filter: Optional dict with column-value pairs for more granular deletion
                          Example: {'PROMPT_TYPE': 'SA_prompt'} will only delete records 
                          matching DATE + DEPARTMENT + PROMPT_TYPE before inserting
        
    Returns:
        dict: Summary of the operation
    """
    
    try:
        # Step 1: Validate dataframe columns match the expected columns
        if len(dataframe.columns) != len(columns):
            raise ValueError(f"Dataframe has {len(dataframe.columns)} columns but expected {len(columns)} columns")
        
        # Step 1: Check if table exists
        try:
            check_query = f"""
            SELECT COUNT(*) AS count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = UPPER('{table_name}')
            AND TABLE_SCHEMA = CURRENT_SCHEMA()
            """
            exists = session.sql(check_query).collect()[0]['COUNT'] > 0
        except:
            exists = False

        # Step 2: Create table if it doesn't exist
        if not exists:
            essential_cols = {
                'DATE': 'DATE',
                'DEPARTMENT': 'VARCHAR(100)',
                'TIMESTAMP': 'TIMESTAMP'
            }

            # Use VARCHAR as default for dynamic columns (customize if needed)
            dynamic_cols = {col_name: 'VARCHAR(16777216)' for col_name in columns}  # Max VARCHAR length in Snowflake

            full_schema = {**essential_cols, **dynamic_cols}
            create_cols_str = ",\n    ".join([f"{col} {dtype}" for col, dtype in full_schema.items()])
            create_query = f"CREATE TABLE {table_name} (\n    {create_cols_str}\n)"
            session.sql(create_query).collect()
            print(f"✅ Created table {table_name} with {len(full_schema)} columns")
        
        # Step 3: Calculate current timestamp
        current_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"Processing data for date: {target_date}")
        print(f"Target table: {table_name}")
        print(f"Department: {department}")
        print(f"Dataframe shape: {dataframe.shape}")
        
        # Step 4: Remove existing rows for yesterday's date with optional additional filters
        delete_conditions = [f"DATE = '{target_date}'", f"DEPARTMENT = '{department}'"]
        
        # Add additional filter conditions if provided
        if additional_filter:
            for col, val in additional_filter.items():
                # Escape single quotes in value
                safe_val = str(val).replace("'", "''")
                delete_conditions.append(f"{col} = '{safe_val}'")
        
        delete_where_clause = " AND ".join(delete_conditions)
        delete_query = f"DELETE FROM {table_name} WHERE {delete_where_clause}"
        
        print(f"⏳ Executing DELETE query...")
        delete_result = session.sql(delete_query).collect()
        print(f"✅ DELETE completed")
        
        filter_desc = f" with {additional_filter}" if additional_filter else ""
        print(f"Cleaned existing data for {target_date} in department {department}{filter_desc}")
        
        # Step 5: Prepare dataframe for insertion
        # Add the essential columns
        dataframe_copy = dataframe.copy()
        dataframe_copy['DATE'] = target_date
        dataframe_copy['TIMESTAMP'] = current_ts
        dataframe_copy['DEPARTMENT'] = department
        
        # Reorder columns to put essential columns first
        essential_cols = ['DATE',  'DEPARTMENT', 'TIMESTAMP']
        dynamic_cols = columns  # The original dynamic columns
        final_column_order = essential_cols + dynamic_cols
        
        # Reorder dataframe columns
        dataframe_copy = dataframe_copy[final_column_order]
        
        # Step 6: Get the actual table column order from Snowflake
        # This ensures we match the table's column order exactly (critical for append mode)
        try:
            table_cols_result = session.sql(f"SHOW COLUMNS IN {table_name}").collect()
            table_column_order = [row['column_name'] for row in table_cols_result]
            
            # Reorder dataframe to match table column order exactly
            # Only include columns that exist in both the dataframe and the table
            matching_cols = [col for col in table_column_order if col in dataframe_copy.columns]
            dataframe_copy = dataframe_copy[matching_cols]
            
            print(f"   📋 Aligned DataFrame columns to match table order: {len(matching_cols)} columns")
        except Exception as e:
            print(f"   ⚠️  Could not align column order (table may not exist yet): {str(e)}")
            # Fall back to original column order if table doesn't exist
            pass
        
        # Step 7: Convert pandas dataframe to Snowpark dataframe and write to table
        print(f"⏳ Converting to Snowpark dataframe...")
        snowpark_df = session.create_dataframe(dataframe_copy)
        
        # Write to table (append mode)
        print(f"⏳ Writing {len(dataframe_copy)} rows to {table_name}...")
        snowpark_df.write.mode("append").save_as_table(table_name)
        print(f"✅ Write completed")
        
        # Step 8: Get final count for verification
        count_query = f"""
        SELECT COUNT(*) as row_count 
        FROM {table_name} 
        WHERE DATE = '{target_date}' AND DEPARTMENT = '{department}'
        """
        
        final_count = session.sql(count_query).collect()[0]['ROW_COUNT']
        
        # Return summary
        summary = {
            "status": "success",
            "table_name": table_name,
            "department": department,
            "date_processed": target_date,
            "timestamp": current_ts,
            "rows_inserted": len(dataframe),
            "final_row_count": final_count,
            "columns_processed": len(columns),
            "total_columns": len(final_column_order)
        }
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Successfully inserted {len(dataframe)} rows into {table_name}", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Final count for {target_date}/{department}: {final_count} rows", flush=True)
        
        return summary
        
    except Exception as e:
        error_summary = {
            "status": "error",
            "table_name": table_name,
            "department": department,
            "error_message": str(e),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        print(f"Error processing data: {str(e)}")
        return error_summary


def analyze_conversations_with_prompt(session, conversations_df, department_name, 
                                    prompt_type, prompt_config, target_date, execution_mode='synchronous'):
    """
    Analyze conversations with a specific prompt and save results using batch processing
    
    Args:
        session: Snowflake session
        conversations_df: DataFrame with conversations (XML, segment, or JSON format)
        department_name: Department name
        prompt_type: Type of prompt being used
        prompt_config: Prompt configuration dictionary
        target_date: Target date for analysis
        execution_mode: 'synchronous' | 'batch_submit' (default: 'synchronous')
    
    Returns:
        Analysis results dictionary
    """
    model_type = prompt_config.get('model_type', 'openai')
    model = prompt_config.get('model', 'gpt-4o-mini')
    conversion_type = prompt_config.get('conversion_type', 'xml')
    system_prompt = prompt_config.get('system_prompt', '')
    
    # Special handling: per-skill prompts (loss_interest for AT_Filipina)
    if prompt_type == 'loss_interest' and isinstance(prompt_config.get('system_prompt'), dict):
        allowed_skills = list(prompt_config['system_prompt'].keys())
        # Filter incoming conversations by last_skill to only allowed skills
        pre_filter_len = len(conversations_df)
        conversations_df = conversations_df[conversations_df['last_skill'].str.upper().isin([skill.upper() for skill in allowed_skills])].copy() if not conversations_df.empty else conversations_df
        print(f"    🔎 loss_interest per-skill: filtered {pre_filter_len}→{len(conversations_df)} by LAST_SKILL in {allowed_skills}")
    
    print(f"    🔍 Preparing {len(conversations_df)} conversations for batch analysis with {prompt_type} using {model_type}/{model} ({conversion_type} format)...")
    
    if conversations_df.empty:
        print(f"    ⚠️  No conversations to analyze for {prompt_type}")
        return {
            'total_conversations': 0,
            'processed_count': 0,
            'prompt_type': prompt_type,
            'conversion_type': conversion_type,
            'model_type': model_type,
            'model_name': model,
            'success_rate': 0,
            'error': 'No conversations to process'
        }
    
    # Step 1: Prepare all records with empty LLM responses
    llm_results_data = []
    
    #Add or remove the test limitations here
    for _, row in conversations_df.iterrows():
        conversation_content = row.get('conversation_content', '')
        execution_id = row.get('execution_id', '')
        is_pending = 'IGNORED' if not execution_id and '@Prompt@' in system_prompt and not department_name.lower().startswith('at_filipina') else 'PENDING'
        
        result_record = {
            'CONVERSATION_ID': row['conversation_id'],
            'SEGMENT_ID': row.get('segment_id', row['conversation_id']),
            'PROMPT_TYPE': prompt_type,
            'CONVERSION_TYPE': conversion_type,
            'MODEL_TYPE': model_type,
            'MODEL_NAME': model,
            'TEMPERATURE': prompt_config.get('temperature', 0.2),
            'MAX_TOKENS': prompt_config.get('max_tokens', 2048),
            'CONVERSATION_CONTENT': conversation_content,
            'LLM_RESPONSE': '',  # Empty initially - will be filled by batch UPDATE
            'LAST_SKILL': row.get('last_skill', ''),
            'CUSTOMER_NAME': row.get('customer_name', ''),
            'AGENT_NAMES': row.get('agent_names', ''),
            'SEGMENT_INDEX': row.get('segment_index', 0),
            'ANALYSIS_DATE': datetime.now().strftime('%Y-%m-%d'),
            'PROCESSING_STATUS': is_pending,  # Will be updated after LLM processing
            'SHADOWED_BY': row.get('shadowed_by', ''),
            'EXECUTION_ID': row.get('execution_id', ''),
            'TOKENS_BREAKDOWN': '',  # Empty initially - will be filled by batch UPDATE
            'IS_PARSED': False,
            'USER_TYPE': row.get('user_type', 'client'),  # User type from XML/JSON converter
        }
        llm_results_data.append(result_record)
    
    # Step 2: Batch insert all records with empty LLM responses
    if not llm_results_data:
        print(f"    ⚠️  No data to process for {prompt_type}")
        return {
            'total_conversations': 0,
            'processed_count': 0,
            'prompt_type': prompt_type,
            'conversion_type': conversion_type,
            'model_type': model_type,
            'model_name': model,
            'success_rate': 0,
            'error': 'No data to process'
        }
    
    try:
        # Insert all records to table
        raw_df = pd.DataFrame(llm_results_data)
        raw_df = clean_dataframe_for_snowflake(raw_df)
        
        dynamic_columns = [col for col in raw_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
        
        insert_success = insert_raw_data_with_cleanup(
            session=session,
            table_name=prompt_config['output_table'],
            department=department_name,
            target_date=target_date,
            dataframe=raw_df[dynamic_columns],
            columns=dynamic_columns
        )
        
        if not insert_success:
            print(f"    ❌ Failed to insert records to {prompt_config['output_table']}")
            return {
                'total_conversations': len(conversations_df),
                'processed_count': 0,
                'prompt_type': prompt_type,
                'conversion_type': conversion_type,
                'model_type': model_type,
                'model_name': model,
                'success_rate': 0,
                'error': 'Failed to insert records'
            }
        
        print(f"    💾 Inserted {len(llm_results_data)} records to {prompt_config['output_table']} for batch processing", flush=True)
        
        # Step 3: Run LLM processing based on execution mode
        if execution_mode == 'batch_submit':
            # BATCH SUBMIT MODE: Submit to OpenAI and return batch_id
            print(f"    [{datetime.now().strftime('%H:%M:%S')}] 📤 Batch submit mode: Submitting to OpenAI Batch API...", flush=True)
            
            batch_success, batch_id, input_file_id, total_requests = run_batch_llm_submit(
                session, prompt_config, department_name, target_date, prompt_type
            )
            
            if not batch_success or not batch_id:
                print(f"    ❌ Batch submission failed for {prompt_type}")
                return {
                    'total_conversations': len(conversations_df),
                    'batch_id': None,
                    'status': 'failed',
                    'prompt_type': prompt_type,
                    'error': 'Batch submission failed'
                }
            
            # Store batch info in BATCH_TRACKING table
            batch_info = {
                'PROMPT_TYPE': prompt_type,
                'BATCH_ID': batch_id,
                'BATCH_STATUS': 'submitted',
                'INPUT_FILE_ID': input_file_id or '',
                'OUTPUT_TABLE': prompt_config['output_table'],
                'MODEL_NAME': model,
                'CONVERSION_TYPE': conversion_type,
                'TOTAL_REQUESTS': total_requests,
                'ERROR_MESSAGE': ''
            }
            
            tracking_success = update_batch_tracking(
                session, target_date, department_name, prompt_type, batch_info
            )
            
            if tracking_success:
                print(f"    [{datetime.now().strftime('%H:%M:%S')}] ✅ Batch tracking updated for {prompt_type}")
            else:
                print(f"    ⚠️  Failed to update batch tracking (batch still submitted)")
            
            # Return batch submission results
            return {
                'total_conversations': len(conversations_df),
                'batch_id': batch_id,
                'input_file_id': input_file_id,
                'total_requests': total_requests,
                'status': 'submitted',
                'prompt_type': prompt_type,
                'conversion_type': conversion_type,
                'model_type': model_type,
                'model_name': model
            }
        
        elif execution_mode == 'external':
            # EXTERNAL MODE: Use llm_clients for parallel HTTP calls (GitHub Actions/Colab)
            print(f"    🌐 External execution mode for {prompt_type}...")
            
            if not is_external_execution_available():
                print(f"    ❌ External execution not available. Falling back to synchronous mode.")
                execution_mode = 'synchronous'  # Fall through to synchronous below
            else:
                batch_success, processed_count, failed_count = run_batch_llm_external(
                    session, prompt_config, department_name, target_date, prompt_type
                )
                
                if not batch_success:
                    print(f"    ❌ External LLM processing failed for {prompt_type}")
                    return {
                        'total_conversations': len(conversations_df),
                        'processed_count': 0,
                        'prompt_type': prompt_type,
                        'conversion_type': conversion_type,
                        'model_type': model_type,
                        'model_name': model,
                        'success_rate': 0,
                        'error': 'External LLM processing failed'
                    }
                
                success_rate = (processed_count / len(conversations_df) * 100) if len(conversations_df) > 0 else 0
                
                return {
                    'total_conversations': len(conversations_df),
                    'processed_count': processed_count,
                    'failed_count': failed_count,
                    'prompt_type': prompt_type,
                    'conversion_type': conversion_type,
                    'model_type': model_type,
                    'model_name': model,
                    'success_rate': success_rate,
                    'execution_mode': 'external'
                }
        
        # SYNCHRONOUS MODE (default): Wait for LLM responses via Snowflake UDFs
        if execution_mode == 'synchronous' or execution_mode not in ('batch_submit', 'external'):
            print(f"    ⏳ Starting LLM batch processing for {prompt_type}... (this may take several minutes)")
            batch_success, processed_count, failed_count = run_batch_llm_update(
                session, prompt_config, department_name, target_date, prompt_type
            )
            
            if not batch_success:
                print(f"    ❌ Batch LLM update failed for {prompt_type}")
                return {
                    'total_conversations': len(conversations_df),
                    'processed_count': 0,
                    'prompt_type': prompt_type,
                    'conversion_type': conversion_type,
                    'model_type': model_type,
                    'model_name': model,
                    'success_rate': 0,
                    'error': 'Batch LLM update failed'
                }
            
            success_rate = (processed_count / len(conversations_df) * 100) if len(conversations_df) > 0 else 0
            
            results = {
                'total_conversations': len(conversations_df),
                'processed_count': processed_count,
                'failed_count': failed_count,
                'prompt_type': prompt_type,
                'conversion_type': conversion_type,
                'model_type': model_type,
                'model_name': model,
                'success_rate': success_rate
            }
            
            print(f"    ✅ {prompt_type} batch processing: {processed_count}/{len(conversations_df)} success ({success_rate:.1f}%), {failed_count} failed")
            
            return results
        
    except Exception as e:
        print(f"    ❌ Error in batch processing for {prompt_type}: {str(e)}")
        return {
            'total_conversations': len(conversations_df),
            'processed_count': 0,
            'prompt_type': prompt_type,
            'conversion_type': conversion_type,
            'model_type': model_type,
            'model_name': model,
            'success_rate': 0,
            'error': f'Batch processing error: {str(e)}'
        }


def build_history_of_chatbot_mv_resolvers(message_segments_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build HistoryOfChatbot for MV_Resolvers message segments.
    
    Takes HISTORY from N8N UDF and stringifies it into MESSAGE JSON.
    
    Args:
        message_segments_df: DataFrame with message segments (already has HISTORY from UDF)
    
    Returns:
        Updated DataFrame with HistoryOfChatbot populated in MESSAGE JSON
    """
    print(f"    🔄 Building HistoryOfChatbot for MV_Resolvers from N8N UDF...")
    
    updated_segments = []
    
    for idx, row in message_segments_df.iterrows():
        # Parse existing MESSAGE JSON
        try:
            message_json = json.loads(row['MESSAGE'])
        except Exception as e:
            print(f"    ⚠️  Error parsing MESSAGE JSON for {row['MESSAGE_ID']}: {str(e)}")
            updated_segments.append(row)
            continue
        
        # Get HISTORY from N8N UDF and stringify it
        history_data = row.get('HISTORY', '[]')
        
        # If it's already a string, use as-is; if it's a dict/list, stringify it
        if isinstance(history_data, str):
            history_string = history_data
        else:
            history_string = json.dumps(history_data)
        
        # Update MESSAGE JSON with HistoryOfChatbot
        message_json['HistoryOfChatbot'] = history_string
        row['MESSAGE'] = json.dumps(message_json)
        
        updated_segments.append(row)
    
    result_df = pd.DataFrame(updated_segments)
    print(f"    ✅ HistoryOfChatbot populated for {len(result_df)} message segments")
    
    return result_df


def analyze_messages_with_tool_eval_prompt(session, message_segments_df, department_name,
                                          prompt_type, prompt_config, target_date, execution_mode='synchronous'):
    """
    Analyze messages with tool evaluation prompt (message-level analysis).
    This is a specialized version of analyze_conversations_with_prompt for tool_eval.
    
    Unlike conversation-level analysis, this:
    1. Converts conversations to message segments (one per consumer message)
    2. Calls GET_N8N_CLIENT_ATTRS_AND_HISTORY UDF for each message
    3. Inserts into TOOL_EVAL_RAW_DATA with MESSAGE_ID and CLIENT_ATTRIBUTES/HISTORY
    
    Args:
        session: Snowflake session
        conversations_df: DataFrame with filtered conversation data
        department_name: Department name (e.g., 'MV_Resolvers')
        prompt_type: Should be 'tool_eval'
        prompt_config: Prompt configuration dictionary
        target_date: Target date for analysis
        execution_mode: 'synchronous' | 'batch_submit' (default: 'synchronous')
    
    Returns:
        Analysis results dictionary
    """
    from snowflake.snowpark.functions import col, lit, call_builtin
    
    model_type = prompt_config.get('model_type', 'openai')
    model = prompt_config.get('model', 'gpt-4o-mini')
    conversion_type = prompt_config.get('conversion_type', 'message_segment')
    
    print(f"    🔍 Converting conversations to message segments for tool evaluation...")
    
    # Step 1: Convert conversations to message segments
    
    if message_segments_df.empty:
        print(f"    ⚠️  No message segments to analyze for {prompt_type}")
        return {
            'total_messages': 0,
            'processed_count': 0,
            'prompt_type': prompt_type,
            'conversion_type': conversion_type,
            'model_type': model_type,
            'model_name': model,
            'success_rate': 0,
            'error': 'No message segments to process'
        }
    
    print(f"    📊 Generated {len(message_segments_df)} message segments from {message_segments_df['CONVERSATION_ID'].nunique()} conversations")
    
    # Step 2: Call N8N UDF to fetch CLIENT_ATTRIBUTES and HISTORY
    print(f"    🔄 Fetching client attributes and history from N8N...")
    
    # Derive department alias from department name (e.g., 'MV_Resolvers' → 'MV_RESOLVERS')
    department_alias = department_name.upper().replace(' ', '_')
    
    try:
        # Create Snowpark DataFrame
        segments_snow_df = session.create_dataframe(message_segments_df)
        
        # Call UDF with both EXECUTION_ID and alias
        print(f"    🔗 Calling GET_N8N_CLIENT_ATTRS_AND_HISTORY with alias='{department_alias}'")
        segments_snow_df = segments_snow_df.with_column(
            'N8N_DATA',
            call_builtin('GET_N8N_CLIENT_ATTRS_AND_HISTORY', col('EXECUTION_ID'), lit(department_alias))
        )
        
        # Extract client_attributes and history from UDF result
        segments_snow_df = segments_snow_df.with_column(
            'CLIENT_ATTRIBUTES',
            col('N8N_DATA')['client_attributes'].cast('VARCHAR')
        ).with_column(
            'HISTORY',
            col('N8N_DATA')['history'].cast('VARCHAR')
        )
        
        # Convert back to Pandas
        message_segments_df = segments_snow_df.to_pandas()
        print(f"    ✅ N8N data fetched for {len(message_segments_df)} message segments")
        
    except Exception as e:
        print(f"    ❌ Error calling N8N UDF: {str(e)}")
        traceback.print_exc()
        return {
            'total_messages': len(message_segments_df),
            'processed_count': 0,
            'prompt_type': prompt_type,
            'error': f'N8N UDF error: {str(e)}'
        }
        
    # Step 2.5: Build HistoryOfChatbot for MV_Resolvers
    if department_name == 'MV_Resolvers':
        print(f"    🔄 Building HistoryOfChatbot for MV_Resolvers...")
        message_segments_df = build_history_of_chatbot_mv_resolvers(message_segments_df)
    
    # Step 3: Prepare records for insertion into TOOL_EVAL_RAW_DATA
    llm_results_data = []
    
    for _, row in message_segments_df.iterrows():
        result_record = {
            'CONVERSATION_ID': row['CONVERSATION_ID'],
            'MESSAGE_ID': row['MESSAGE_ID'],
            'PROMPT_TYPE': prompt_type,
            'CONVERSION_TYPE': conversion_type,
            'MODEL_TYPE': model_type,
            'MODEL_NAME': model,
            'TEMPERATURE': prompt_config.get('temperature', 0.2),
            'MAX_TOKENS': prompt_config.get('max_tokens', 2048),
            'MESSAGE': row['MESSAGE'],
            'CLIENT_ATTRIBUTES': row.get('CLIENT_ATTRIBUTES', '{}'),
            'HISTORY': row.get('HISTORY', '[]'),
            'ACTUAL_TOOLS_CALLED': row['ACTUAL_TOOLS_CALLED'],
            'LLM_RESPONSE': '',  # Empty initially - will be filled by batch UPDATE
            'TARGET_SKILL': row['TARGET_SKILL'],
            'CUSTOMER_NAME': row['CUSTOMER_NAME'],
            'AGENT_NAMES': '',  # Not applicable for message-level
            'PROCESSING_STATUS': 'PENDING',
            'SHADOWED_BY': row.get('SHADOWED_BY', ''),
            'EXECUTION_ID': row.get('EXECUTION_ID', ''),
            'TOKENS_BREAKDOWN': '',  # Empty initially
            'IS_PARSED': False,
            'USER_TYPE': row.get('USER_TYPE', 'client')
        }
        llm_results_data.append(result_record)
    
    if not llm_results_data:
        print(f"    ⚠️  No data to process for {prompt_type}")
        return {
            'total_messages': 0,
            'processed_count': 0,
            'prompt_type': prompt_type,
            'error': 'No data to process'
        }
    
    try:
        # Step 4: Insert records to TOOL_EVAL_RAW_DATA
        raw_df = pd.DataFrame(llm_results_data)
        raw_df = clean_dataframe_for_snowflake(raw_df)
        
        dynamic_columns = [col for col in raw_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
        
        insert_success = insert_raw_data_with_cleanup(
            session=session,
            table_name=prompt_config['output_table'],
            department=department_name,
            target_date=target_date,
            dataframe=raw_df[dynamic_columns],
            columns=dynamic_columns
        )
        
        if not insert_success:
            print(f"    ❌ Failed to insert records to {prompt_config['output_table']}")
            return {
                'total_messages': len(message_segments_df),
                'processed_count': 0,
                'prompt_type': prompt_type,
                'error': 'Failed to insert records'
            }
        
        print(f"    💾 Inserted {len(llm_results_data)} records to {prompt_config['output_table']}")
        
        # Step 5: Run LLM processing based on execution mode
        if execution_mode == 'batch_submit':
            # BATCH SUBMIT MODE
            print(f"    📤 Batch submit mode: Submitting to OpenAI Batch API...")
            
            batch_success, batch_id, input_file_id, total_requests = run_batch_llm_submit(
                session, prompt_config, department_name, target_date, prompt_type
            )
            
            if not batch_success or not batch_id:
                print(f"    ❌ Batch submission failed for {prompt_type}")
                return {
                    'total_messages': len(message_segments_df),
                    'batch_id': None,
                    'status': 'failed',
                    'error': 'Batch submission failed'
                }
            
            # Store batch info
            batch_info = {
                'PROMPT_TYPE': prompt_type,
                'BATCH_ID': batch_id,
                'BATCH_STATUS': 'submitted',
                'INPUT_FILE_ID': input_file_id or '',
                'OUTPUT_TABLE': prompt_config['output_table'],
                'MODEL_NAME': model,
                'CONVERSION_TYPE': conversion_type,
                'TOTAL_REQUESTS': total_requests,
                'ERROR_MESSAGE': ''
            }
            
            tracking_success = update_batch_tracking(
                session, target_date, department_name, prompt_type, batch_info
            )
            
            if tracking_success:
                print(f"    ✅ Batch tracking updated for {prompt_type}")
            else:
                print(f"    ⚠️  Failed to update batch tracking (batch still submitted)")
            
            return {
                'total_messages': len(message_segments_df),
                'batch_id': batch_id,
                'input_file_id': input_file_id,
                'total_requests': total_requests,
                'status': 'submitted',
                'prompt_type': prompt_type
            }
        
        else:
            # SYNCHRONOUS MODE
            batch_success, processed_count, failed_count = run_batch_llm_update(
                session, prompt_config, department_name, target_date, prompt_type
            )
            
            if not batch_success:
                print(f"    ❌ Batch LLM update failed for {prompt_type}")
                return {
                    'total_messages': len(message_segments_df),
                    'processed_count': 0,
                    'error': 'Batch LLM update failed'
                }
            
            success_rate = (processed_count / len(message_segments_df) * 100) if len(message_segments_df) > 0 else 0
            
            print(f"    ✅ {prompt_type}: {processed_count}/{len(message_segments_df)} success ({success_rate:.1f}%)")
            
            return {
                'total_messages': len(message_segments_df),
                'processed_count': processed_count,
                'failed_count': failed_count,
                'prompt_type': prompt_type,
                'success_rate': success_rate
            }
    
    except Exception as e:
        print(f"    ❌ Error in tool eval processing: {str(e)}")
        traceback.print_exc()
        return {
            'total_messages': len(message_segments_df) if not message_segments_df.empty else 0,
            'processed_count': 0,
            'error': f'Processing error: {str(e)}'
        }


# def run_batch_llm_update(session: snowpark.Session, prompt_config, department_name, target_date):
#     """
#     Run LLM analysis using the OpenAI Batch API via the openai_chat_system UDF (batch_* modes).
#     Falls back to the existing synchronous path for Gemini.

#     Returns: (success: bool, processed_count: int, failed_count: int)
#     """
#     import time
#     from datetime import datetime

#     total_start_time = time.time()

#     try:
#         # ---------- Setup ----------
#         setup_start = time.time()
#         table_name   = prompt_config['output_table']
#         model_type   = prompt_config.get('model_type', 'openai').lower()
#         system_text  = prompt_config.get('system_prompt', '')
#         per_skill    = isinstance(system_text, dict)
#         reasoning_effort = prompt_config.get('reasoning_effort', 'minimal')

#         # choose UDF name
#         if model_type == 'openai':
#             llm_function = 'openai_chat_system'
#         elif model_type == 'gemini':
#             llm_function = 'gemini_chat_system'  # remains synchronous (no batch mode)
#         else:
#             print(f"    ❌ Unsupported model_type: {model_type}")
#             return False, 0, 0

#         # normalize date
#         target_date_str = target_date if target_date else datetime.now().strftime("%Y-%m-%d")
#         setup_time = time.time() - setup_start
#         print(f"    🔧 Setup completed in {setup_time:.2f}s")

#         # ---------- Count pending ----------
#         count_start = time.time()
#         if per_skill:
#             allowed_skills = [str(s).upper() for s in system_text.keys()]
#             skill_list_sql = ", ".join(["'{}'".format(s.replace("'", "''")) for s in allowed_skills])
#             count_sql = f"""
#                 SELECT COUNT(*) AS pending_count
#                 FROM {table_name}
#                 WHERE PROCESSING_STATUS = 'PENDING'
#                   AND DEPARTMENT = '{department_name}'
#                   AND DATE = '{target_date_str}'
#                   AND UPPER(LAST_SKILL) IN ({skill_list_sql})
#             """
#         else:
#             count_sql = f"""
#                 SELECT COUNT(*) AS pending_count
#                 FROM {table_name}
#                 WHERE PROCESSING_STATUS = 'PENDING'
#                   AND DEPARTMENT = '{department_name}'
#                   AND DATE = '{target_date_str}'
#             """

#         pending_count = session.sql(count_sql).collect()[0]['PENDING_COUNT']
#         count_time = time.time() - count_start
#         print(f"    📊 Found {pending_count} pending records (in {count_time:.2f}s)")
#         if pending_count == 0:
#             print(f"    ⚠️  No pending records for {department_name}")
#             return True, 0, 0

#         print(f"    🚀 Running {model_type.upper()} analysis on {pending_count} records...")

#         # ---------- Branch: Gemini stays synchronous ----------
#         if model_type == 'gemini':
#             print(f"    🚀 Running batch {model_type.upper()} analysis on {pending_count} records...")
            
#             # Step 3: Build and execute the UPDATE query
#             query_build_start_time = time.time()
            
#             # BATCH SQL APPROACH (5x faster than UPDATE)
#             query_build_time = time.time() - query_build_start_time
#             print(f"    📝 Batch SQL approach selected (5x faster)")
            
#             execution_start_time = time.time()
#             print(f"    ⏳ Starting LLM batch execution... (estimated: {pending_count * 0.4}s+ for {pending_count} records)")
            
#             try:
#                 # Step 1: Check if system prompt contains @Prompt@ placeholder
#                 needs_prompt_replacement = '@Prompt@' in (system_text if not per_skill else "".join(system_text.values()))
                
#                 if needs_prompt_replacement or per_skill:
#                     print(f"    🔄 System prompt contains @Prompt@ - fetching conversation-specific prompts...")
                    
#                     # Create batch processing query with dynamic system prompt replacement
#                     if per_skill:
#                         # Build CASE expression selecting prompt by LAST_SKILL
#                         departments_config_full = get_snowflake_llm_departments_config()
#                         # allowed skills based on keys
#                         allowed_skills = [str(s).upper() for s in system_text.keys()]
#                         skill_list_sql = ", ".join(["'" + s.replace("'", "''") + "'" for s in allowed_skills])

#                         case_branches = []
#                         for skill, prompt in system_text.items():
#                             safe_skill = str(skill).upper().replace("'", "''")
#                             safe_prompt = prompt  # will be dollar-quoted
#                             case_branches.append(f"WHEN UPPER(LAST_SKILL) = '{safe_skill}' THEN $${safe_prompt}$$")
#                         case_expr = "CASE " + " ".join(case_branches) + " END"

#                         batch_query = f"""
#                         WITH batch_processing AS (
#                             SELECT 
#                                 CONVERSATION_ID,
#                                 {llm_function}(
#                                     CONVERSATION_CONTENT,
#                                     REPLACE(
#                                         REPLACE({case_expr}, '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')),
#                                         '<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>')
#                                     ),
#                                     MODEL_NAME,
#                                     TEMPERATURE,
#                                     MAX_TOKENS,
#                                     '{prompt_config.get('reasoning_effort', 'minimal')}',
#                                     '{department_name}'
#                                 ) AS llm_result
#                             FROM {table_name}
#                             WHERE PROCESSING_STATUS = 'PENDING'
#                             AND DEPARTMENT = '{department_name}'
#                             AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
#                             AND UPPER(LAST_SKILL) IN ({skill_list_sql})
#                         )
#                         SELECT 
#                             CONVERSATION_ID, 
#                             llm_result:text::string AS LLM_RESPONSE,
#                             llm_result:usage        AS TOKENS_BREAKDOWN
#                         FROM batch_processing
#                         """
#                     else:
#                         batch_query = f"""
#                     WITH batch_processing AS (
#                         SELECT 
#                             CONVERSATION_ID,
#                             {llm_function}(
#                                 CONVERSATION_CONTENT,
#                                 REPLACE(
#                                     REPLACE($${system_text}$$, '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')),
#                                     '<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>')
#                                 ),
#                                 MODEL_NAME,
#                                 TEMPERATURE,
#                                 MAX_TOKENS,
#                                 '{prompt_config.get('reasoning_effort', 'minimal')}',
#                                 '{department_name}'
#                             ) AS llm_result
#                         FROM {table_name}
#                         WHERE PROCESSING_STATUS = 'PENDING'
#                         AND DEPARTMENT = '{department_name}'
#                             AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
#                     )
#                     SELECT 
#                         CONVERSATION_ID, 
#                         llm_result:text::string AS LLM_RESPONSE,
#                         llm_result:usage        AS TOKENS_BREAKDOWN
#                     FROM batch_processing
#                     """
#                 else:
#                     # Original batch processing without prompt replacement
#                     if per_skill:
#                         allowed_skills = [str(system_text.keys())]
#                         skill_list_sql = ", ".join(["'" + str(s).upper().replace("'", "''") + "'" for s in system_text.keys()])
#                         case_branches = []
#                         for skill, prompt in system_text.items():
#                             safe_skill = str(skill).upper().replace("'", "''")
#                             safe_prompt = prompt
#                             case_branches.append(f"WHEN UPPER(LAST_SKILL) = '{safe_skill}' THEN $${safe_prompt}$$")
#                         case_expr = "CASE " + " ".join(case_branches) + " END"
#                         batch_query = f"""
#                         WITH batch_processing AS (
#                             SELECT 
#                                 CONVERSATION_ID,
#                                 {llm_function}(
#                                     CONVERSATION_CONTENT,
#                                     {case_expr},
#                                     MODEL_NAME,
#                                     TEMPERATURE,
#                                     MAX_TOKENS,
#                                     '{prompt_config.get('reasoning_effort', 'minimal')}',
#                                     '{department_name}'
#                                 ) AS llm_result
#                             FROM {table_name}
#                             WHERE PROCESSING_STATUS = 'PENDING'
#                             AND DEPARTMENT = '{department_name}'
#                             AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
#                             AND UPPER(LAST_SKILL) IN ({skill_list_sql})
#                         )
#                         SELECT 
#                             CONVERSATION_ID, 
#                             llm_result:text::string AS LLM_RESPONSE,
#                             llm_result:usage        AS TOKENS_BREAKDOWN
#                         FROM batch_processing
#                         """
#                     else:
#                         batch_query = f"""
#                     WITH batch_processing AS (
#                         SELECT 
#                             CONVERSATION_ID,
#                             {llm_function}(
#                                 CONVERSATION_CONTENT,
#                                 $${system_text}$$,
#                                 MODEL_NAME,
#                                 TEMPERATURE,
#                                 MAX_TOKENS,
#                                 '{prompt_config.get('reasoning_effort', 'minimal')}',
#                                 '{department_name}'
#                             ) AS llm_result
#                         FROM {table_name}
#                         WHERE PROCESSING_STATUS = 'PENDING'
#                         AND DEPARTMENT = '{department_name}'
#                         AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
#                     )
#                     SELECT 
#                         CONVERSATION_ID, 
#                         llm_result:text::string AS LLM_RESPONSE,
#                         llm_result:usage        AS TOKENS_BREAKDOWN
#                     FROM batch_processing
#                     """
                
#                 # Execute batch processing
#                 batch_results = session.sql(batch_query).collect()
            
#                 if not batch_results:
#                     print(f"    ⚠️  No results from batch processing")
#                     execution_time = time.time() - execution_start_time
#                 else:
#                     # Step 2: Update original table with batch results
#                     print(f"    🔄 Updating {len(batch_results)} records with LLM responses...")
                    
#                     # Create temp table with results for efficient UPDATE
#                     temp_results_table = f"TEMP_BATCH_RESULTS_{int(time.time())}"
                    
#                     # Convert results to DataFrame
#                     results_data = [
#                         {
#                             'CONVERSATION_ID': row['CONVERSATION_ID'],
#                             'LLM_RESPONSE': row['LLM_RESPONSE'],
#                             'TOKENS_BREAKDOWN': row['TOKENS_BREAKDOWN']
#                         }
#                         for row in batch_results
#                     ]
#                     results_df = session.create_dataframe(results_data)
#                     results_df.write.mode("overwrite").save_as_table(temp_results_table)
                    
#                     # Update original table using JOIN on CONVERSATION_ID
#                     update_query = f"""
#                     UPDATE {table_name}
#                     SET 
#                         LLM_RESPONSE = temp.LLM_RESPONSE,
#                         TOKENS_BREAKDOWN = temp.TOKENS_BREAKDOWN,
#                         PROCESSING_STATUS = 'COMPLETED'
#                     FROM {temp_results_table} temp
#                     WHERE {table_name}.CONVERSATION_ID = temp.CONVERSATION_ID
#                     """
                    
#                     session.sql(update_query).collect()
                    
#                     # Clean up temp table
#                     session.sql(f"DROP TABLE {temp_results_table}").collect()
                    
#                     execution_time = time.time() - execution_start_time
#                     print(f"    ✅ Batch SQL approach completed successfully")
                    
#             except Exception as batch_error:
#                 print(f"    ⚠️  Batch SQL failed, falling back to UPDATE method: {str(batch_error)}")
                
#                 # Fallback to original UPDATE method with prompt replacement support
#                 if needs_prompt_replacement or per_skill:
#                     print(f"    🔄 Using UPDATE method with @Prompt@ replacement...")
#                     if per_skill:
#                         allowed_skills = [str(s).upper() for s in system_text.keys()]
#                         skill_list_sql = ", ".join(["'" + s.replace("'", "''") + "'" for s in allowed_skills])
#                         case_branches = []
#                         for skill, prompt in system_text.items():
#                             safe_skill = str(skill).upper().replace("'", "''")
#                             safe_prompt = prompt
#                             case_branches.append(
#                                 f"WHEN UPPER(LAST_SKILL) = '{safe_skill}' THEN REPLACE($${safe_prompt}$$, '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT({table_name}.EXECUTION_ID, '{department_name}'), '@Prompt@'))"
#                             )
#                         case_expr = "CASE " + " ".join(case_branches) + " END"
#                         update_query = f"""
#                         UPDATE {table_name}
#                         SET 
#                             LLM_RESPONSE = (
#                                 {llm_function}(
#                                     CONVERSATION_CONTENT,
#                                     REPLACE({case_expr}, '<STEP-NAME>', COALESCE({table_name}.LAST_SKILL, '<STEP-NAME>')),
#                                     MODEL_NAME,
#                                     TEMPERATURE,
#                                     MAX_TOKENS,
#                                     '{prompt_config.get('reasoning_effort', 'minimal')}',
#                                     '{department_name}'
#                                 ):text::string
#                             ),
#                             TOKENS_BREAKDOWN = '',
#                             PROCESSING_STATUS = 'COMPLETED'
#                         WHERE PROCESSING_STATUS = 'PENDING'
#                         AND DEPARTMENT = '{department_name}'
#                         AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
#                         AND LAST_SKILL IN ({skill_list_sql})
#                         """
#                     else:
#                         update_query = f"""
#                         UPDATE {table_name}
#                         SET 
#                             LLM_RESPONSE = (
#                                 {llm_function}(
#                                     CONVERSATION_CONTENT,
#                                     REPLACE(
#                                         REPLACE($${system_text}$$, '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT({table_name}.EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT({table_name}.CONVERSATION_ID), '@Prompt@')),
#                                         '<STEP-NAME>', COALESCE({table_name}.LAST_SKILL, '<STEP-NAME>')
#                                     ),
#                                     MODEL_NAME,
#                                     TEMPERATURE,
#                                     MAX_TOKENS,
#                                     '{prompt_config.get('reasoning_effort', 'minimal')}',
#                                     '{department_name}'
#                                 ):text::string
#                             ),
#                             TOKENS_BREAKDOWN = '',
#                             PROCESSING_STATUS = 'COMPLETED'
#                         WHERE PROCESSING_STATUS = 'PENDING'
#                         AND DEPARTMENT = '{department_name}'
#                         AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
#                         """
#                 else:
#                     # Original UPDATE method without prompt replacement
#                     update_query = f"""
#                     UPDATE {table_name}
#                     SET 
#                         LLM_RESPONSE = (
#                             {llm_function}(
#                                 CONVERSATION_CONTENT,
#                                 $${system_text}$$,
#                                 MODEL_NAME,
#                                 TEMPERATURE,
#                                 MAX_TOKENS,
#                                 '{prompt_config.get('reasoning_effort', 'minimal')}',
#                                 '{department_name}'
#                             ):text::string
#                         ),
#                         TOKENS_BREAKDOWN = '',
#                         PROCESSING_STATUS = 'COMPLETED'
#                     WHERE PROCESSING_STATUS = 'PENDING'
#                     AND DEPARTMENT = '{department_name}'
#                     AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
#                     """
            
#             result = session.sql(update_query).collect()
#             execution_time = time.time() - execution_start_time
#             print(f"    ✅ Fallback UPDATE method completed")
#             records_per_second = pending_count / execution_time if execution_time > 0 else 0
            
#             print(f"    ✅ Batch UPDATE executed in {execution_time:.2f}s")
#             print(f"    📈 Performance: {records_per_second:.2f} records/second")
#             print(f"    🔍 Average time per record: {execution_time/pending_count:.2f}s" if pending_count > 0 else "")
            
#             # Step 4: Count successes and failures with timing
#             counting_start_time = time.time()
            
#             # Narrow counting for per-skill mode to the allowed skill subset
#             if per_skill:
#                 allowed_skills = [str(s).upper() for s in system_text.keys()]
#                 skill_list_sql = ", ".join(["'" + s.replace("'", "''") + "'" for s in allowed_skills])
#                 processed_count, failed_count = count_llm_results_with_extra_filter(
#                     session, table_name, department_name, target_date, f"AND UPPER(LAST_SKILL) IN ({skill_list_sql})"
#                 )
#             else:
#                 processed_count, failed_count = count_llm_results(session, table_name, department_name, target_date)
            
#             counting_time = time.time() - counting_start_time
#             print(f"    📊 Results counted in {counting_time:.2f}s")
            
#             # Total timing summary
#             total_time = time.time() - total_start_time
#             print(f"    🏁 TOTAL BATCH TIME: {total_time:.2f}s for {pending_count} records")
#             print(f"    📋 Time breakdown:")
#             print(f"       - Setup: {setup_time:.2f}s ({setup_time/total_time*100:.1f}%)")
#             print(f"       - Counting: {count_time:.2f}s ({count_time/total_time*100:.1f}%)")
#             print(f"       - Query Build: {query_build_time:.3f}s ({query_build_time/total_time*100:.1f}%)")
#             print(f"       - LLM Execution: {execution_time:.2f}s ({execution_time/total_time*100:.1f}%)")
#             print(f"       - Results: {counting_time:.2f}s ({counting_time/total_time*100:.1f}%)")
            
#             # Performance insights
#             if execution_time > 30:
#                 print(f"    ⚠️  SLOW EXECUTION DETECTED!")
#                 print(f"       - Consider reducing batch size or checking Snowflake compute resources")
#                 print(f"       - {model_type.upper()} may be rate-limited or overloaded")
            
#             if records_per_second < 0.5:
#                 print(f"    🐌 LOW THROUGHPUT WARNING: {records_per_second:.2f} records/second")
#                 print(f"       - Expected: 1-5 records/second for typical LLM operations")
            
#             return True, processed_count, failed_count
            
#         # ---------- OpenAI Batch path ----------
#         query_build_start = time.time()

#         # Build common base, then CASE-ed instructions per row
#         if per_skill:
#             allowed_skills = [str(s).upper() for s in system_text.keys()]
#             skill_list_sql = ", ".join(["'{}'".format(s.replace("'", "''")) for s in allowed_skills])

#             # CASE branches -> system prompts per LAST_SKILL (dollar-quoted)
#             case_branches = []
#             for skill, prompt in system_text.items():
#                 safe_skill = str(skill).upper().replace("'", "''")
#                 safe_prompt = prompt  # will be $$...$$
#                 case_branches.append(f"WHEN UPPER(LAST_SKILL) = '{safe_skill}' THEN $${safe_prompt}$$")
#             case_expr = "CASE " + " ".join(case_branches) + " END"

#             # items[] for batch_submit: {custom_id, input, instructions}
#             # We also compute a single model/temperature/max_tokens to use for this batch.
#             # If you truly have multiple models in the same department/date, consider splitting into separate batches.
#             batch_submit_sql = f"""
#                 WITH base AS (
#                 SELECT
#                     CONVERSATION_ID,
#                     CONVERSATION_CONTENT,
#                     LAST_SKILL,
#                     EXECUTION_ID,
#                     MODEL_NAME,
#                     TEMPERATURE,
#                     MAX_TOKENS
#                 FROM {table_name}
#                 WHERE PROCESSING_STATUS = 'PENDING'
#                     AND DEPARTMENT = '{department_name}'
#                     AND DATE = '{target_date_str}'
#                     AND UPPER(LAST_SKILL) IN ({skill_list_sql})
#                 ),
#                 prompts AS (
#                 SELECT
#                     CONVERSATION_ID,
#                     CONVERSATION_CONTENT,
#                     REPLACE(
#                     REPLACE(
#                         {case_expr},
#                         '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')
#                     ),
#                     '<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>')
#                     ) AS INSTRUCTIONS,
#                     MODEL_NAME,
#                     TEMPERATURE,
#                     MAX_TOKENS
#                 FROM base
#                 ),
#                 model_params AS (
#                 SELECT
#                     ANY_VALUE(MODEL_NAME)    AS MODEL_NAME,
#                     ANY_VALUE(TEMPERATURE)   AS TEMPERATURE,
#                     ANY_VALUE(MAX_TOKENS)    AS MAX_TOKENS
#                 FROM prompts
#                 ),
#                 items AS (
#                 SELECT
#                     ARRAY_AGG(
#                     OBJECT_CONSTRUCT(
#                         'custom_id',    CONVERSATION_ID,
#                         'input',        CONVERSATION_CONTENT,
#                         'instructions', INSTRUCTIONS
#                     )
#                     ) WITHIN GROUP (ORDER BY CONVERSATION_ID) AS arr
#                 FROM prompts
#                 ),
#                 submit AS (
#                 SELECT openai_chat_system(
#                         NULL,
#                         NULL,
#                         (SELECT MODEL_NAME  FROM model_params),
#                         (SELECT TEMPERATURE FROM model_params),
#                         (SELECT MAX_TOKENS  FROM model_params),
#                         '{reasoning_effort}',
#                         'test',
#                         'batch_submit',
#                         TO_JSON(arr),
#                         NULL
#                         ) AS resp
#                 FROM items
#                 )
#                 SELECT
#                 resp:batch_id::string      AS BATCH_ID,
#                 resp:status::string        AS BATCH_STATUS,
#                 resp:input_file_id::string AS INPUT_FILE_ID
#                 FROM submit
#             """
#         else:
#             # Single system prompt for all rows (still doing @Prompt@ and <STEP-NAME> per row)
#             batch_submit_sql = f"""
#                 WITH base AS (
#                 SELECT
#                     CONVERSATION_ID,
#                     CONVERSATION_CONTENT,
#                     LAST_SKILL,
#                     EXECUTION_ID,
#                     MODEL_NAME,
#                     TEMPERATURE,
#                     MAX_TOKENS
#                 FROM {table_name}
#                 WHERE PROCESSING_STATUS = 'PENDING'
#                     AND DEPARTMENT = '{department_name}'
#                     AND DATE = '{target_date_str}'
#                 ),
#                 prompts AS (
#                 SELECT
#                     CONVERSATION_ID,
#                     CONVERSATION_CONTENT,
#                     REPLACE(
#                     REPLACE(
#                         $${system_text}$$,
#                         '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')
#                     ),
#                     '<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>')
#                     ) AS INSTRUCTIONS,
#                     MODEL_NAME,
#                     TEMPERATURE,
#                     MAX_TOKENS
#                 FROM base
#                 ),
#                 model_params AS (
#                 SELECT
#                     ANY_VALUE(MODEL_NAME)    AS MODEL_NAME,
#                     ANY_VALUE(TEMPERATURE)   AS TEMPERATURE,
#                     ANY_VALUE(MAX_TOKENS)    AS MAX_TOKENS
#                 FROM prompts
#                 ),
#                 items AS (
#                 SELECT
#                     ARRAY_AGG(
#                     OBJECT_CONSTRUCT(
#                         'custom_id',    CONVERSATION_ID,
#                         'input',        CONVERSATION_CONTENT,
#                         'instructions', INSTRUCTIONS
#                     )
#                     ) WITHIN GROUP (ORDER BY CONVERSATION_ID) AS arr
#                 FROM prompts
#                 ),
#                 submit AS (
#                 SELECT openai_chat_system(
#                         NULL,
#                         NULL,
#                         (SELECT MODEL_NAME  FROM model_params),
#                         (SELECT TEMPERATURE FROM model_params),
#                         (SELECT MAX_TOKENS  FROM model_params),
#                         '{reasoning_effort}',
#                         'test',
#                         'batch_submit',
#                         TO_JSON(arr),
#                         NULL
#                         ) AS resp
#                 FROM items
#                 )
#                 SELECT
#                 resp:batch_id::string      AS BATCH_ID,
#                 resp:status::string        AS BATCH_STATUS,
#                 resp:input_file_id::string AS INPUT_FILE_ID
#                 FROM submit

#             """

#         # Submit
#         submit_row = session.sql(batch_submit_sql).collect()[0]
#         batch_id   = submit_row['BATCH_ID']
#         batch_stat = submit_row['BATCH_STATUS']
#         print(f"    📨 Batch submitted: id={batch_id}, status={batch_stat}")

#         query_build_time = time.time() - query_build_start
#         print(f"    📝 Built & submitted batch in {query_build_time:.2f}s")

#         # ---------- Poll status ----------
#         poll_start = time.time()
#         terminal_statuses = {"completed", "failed", "canceled", "expired"}
#         sleep_s = 10
#         max_wait_s = 72000  # heuristic window

#         while True:
#             status_sql = f"""
#             WITH r AS (
#             SELECT openai_chat_system(
#                     NULL, NULL,
#                     'gpt-5-mini', 0, 0, '{reasoning_effort}',
#                     'test',
#                     'batch_status',
#                     NULL,
#                     '{batch_id}'
#                     ) AS v
#             )
#             SELECT
#             v:status::string                 AS STATUS,
#             v:request_counts                 AS REQUEST_COUNTS,
#             v:error                          AS ERROR,
#             v                                 AS RAW
#             FROM r
#             """
#             srow = session.sql(status_sql).collect()[0]
#             st   = (srow['STATUS'] or '').lower()
#             rc   = srow['REQUEST_COUNTS']
#             err  = srow['ERROR']
#             print(f"    ⏱️  Batch status: {st} | request_counts={rc} | error={err}")

#             if st in terminal_statuses:
#                 batch_stat = st
#                 break
#             if (time.time() - poll_start) > max_wait_s:
#                 print("    ⏳ Poll timeout reached; stopping polling.")
#                 break
#             time.sleep(sleep_s)


#         if batch_stat != 'completed':
#             print(f"    ❌ Batch finished with status '{batch_stat}'.")
#             return False, 0, pending_count

#         # ---------- Fetch results and MERGE back ----------
#         fetch_sql = f"""
#         MERGE INTO {table_name} AS T
#         USING (
#         SELECT
#             f.value:custom_id::string AS CONVERSATION_ID,
#             f.value:text::string      AS LLM_RESPONSE,
#             TO_VARCHAR(f.value:usage) AS TOKENS_BREAKDOWN
#         FROM (
#             SELECT openai_chat_system(
#                     NULL, NULL,
#                     'gpt-5-mini', 0, 0, '{reasoning_effort}',
#                     'test',
#                     'batch_result',
#                     NULL,
#                     '{batch_id}'
#                 ) AS v
#         ) r,
#         LATERAL FLATTEN(input => r.v:results) f
#         ) AS S
#         ON T.CONVERSATION_ID = S.CONVERSATION_ID
#         WHEN MATCHED THEN UPDATE SET
#         T.LLM_RESPONSE      = S.LLM_RESPONSE,
#         T.TOKENS_BREAKDOWN  = S.TOKENS_BREAKDOWN,
#         T.PROCESSING_STATUS = 'COMPLETED',
#         T.DATE        = DATE('{target_date_str}');
#         """


#         session.sql(fetch_sql).collect()
#         print(f"    ✅ Results merged back for batch {batch_id}")

#         # ---------- Count results ----------
#         # Narrow counting for per-skill mode to the allowed skill subset
#         if per_skill:
#             allowed_skills = [str(s).upper() for s in system_text.keys()]
#             skill_list_sql = ", ".join(["'{}'".format(s.replace("'", "''")) for s in allowed_skills])
#             # processed = COMPLETED rows in this subset
#             processed_sql = f"""
#               SELECT
#                 SUM(IFF(PROCESSING_STATUS='COMPLETED',1,0)) AS PROCESSED,
#                 SUM(IFF(PROCESSING_STATUS='FAILED',1,0))     AS FAILED
#               FROM {table_name}
#               WHERE DEPARTMENT='{department_name}'
#                 AND DATE='{target_date_str}'
#                 AND UPPER(LAST_SKILL) IN ({skill_list_sql})
#             """
#         else:
#             processed_sql = f"""
#               SELECT
#                 SUM(IFF(PROCESSING_STATUS='COMPLETED',1,0)) AS PROCESSED,
#                 SUM(IFF(PROCESSING_STATUS='FAILED',1,0))     AS FAILED
#               FROM {table_name}
#               WHERE DEPARTMENT='{department_name}'
#                 AND DATE='{target_date_str}'
#             """
#         pc = session.sql(processed_sql).collect()[0]
#         processed_count = int(pc['PROCESSED'] or 0)
#         failed_count    = int(pc['FAILED'] or 0)

#         # ---------- Timing / perf ----------
#         total_time = time.time() - total_start_time
#         records_per_second = (processed_count / total_time) if total_time > 0 else 0.0

#         print(f"    🏁 TOTAL BATCH TIME: {total_time:.2f}s for {pending_count} records")
#         print(f"    📈 Throughput: {records_per_second:.2f} rows/sec")
#         if total_time > 30:
#             print("    ⚠️ Consider smaller batches / larger warehouse for faster end-to-end time.")

#         return True, processed_count, failed_count

#     except Exception as e:
#         total_time = time.time() - total_start_time
#         print(f"    ❌ Batch LLM update failed after {total_time:.2f}s: {e}")
#         # If you have your own formatter:
#         try:
#             print(format_error_details(e, f"BATCH LLM UPDATE - {department_name}"))
#         except Exception:
#             pass
#         return False, 0, 0


def run_batch_llm_update(session: snowpark.Session, prompt_config, department_name, target_date, prompt_type=None):
    """
    Run batch UPDATE query to fill LLM responses using Snowflake's LLM functions
    
    Args:
        session: Snowflake session
        prompt_config: Prompt configuration dictionary
        department_name: Department name
        target_date: Target date for analysis
        prompt_type: Prompt type (e.g., 'tool_eval', 'SA_prompt', etc.)
    
    Returns:
        Tuple: (success, processed_count, failed_count)
    """
    import time
    
    # Start total timing
    total_start_time = time.time()
    
    try:
        # Step 1: Setup and validation timing
        setup_start_time = time.time()
        
        table_name = prompt_config['output_table']
        model_type = prompt_config.get('model_type', 'openai').lower()
        departments_config = get_snowflake_base_departments_config()
        gpt_agent_name = departments_config[department_name]['GPT_AGENT_NAME'].replace("'", "''")
        
        # Extract response_format from config
        response_format = prompt_config.get('response_format', None)
        response_format_param = f"'{response_format}'" if response_format else 'NULL'
        
        # Choose the appropriate LLM function based on model_type AND prompt_type
        dynamic_prompt_udf = prompt_config.get('dynamic_prompt_udf')
        
        if model_type == 'openai':
            # For tool_eval, check if dynamic UDF is configured
            if prompt_type == 'tool_eval':
                if dynamic_prompt_udf:
                    # MV_Sales mode: Chat Completions API with dynamic UDF
                    llm_function = 'openai_chat_system_completions'
                    print(f"    🎯 MV_Sales mode: Using Chat Completions API (openai_chat_system_completions)")
                else:
                    # MV_Resolvers mode: Responses API with static prompt
                    llm_function = 'openai_chat_system'
                    print(f"    🎯 MV_Resolvers mode: Using Responses API (openai_chat_system)")
            else:
                llm_function = 'openai_chat_system'
                print(f"    🎯 Using Responses API (openai_chat_system) for {prompt_type}")
        elif model_type == 'gemini':
            llm_function = 'gemini_chat_system'
        else:
            print(f"    ❌ Unsupported model_type: {model_type}")
            return False, 0, 0
        
        # Use the original prompts - dollar-quoted strings handle all special characters safely
        prompt_text = prompt_config['prompt']
        system_text = prompt_config['system_prompt']
        per_skill_mode = isinstance(system_text, dict)  # loss_interest per LAST_SKILL
        per_user_type_mode = prompt_config.get('per_user_type_mode', False)  # threatening per USER_TYPE
        system_text_by_user_type = prompt_config.get('system_prompt_by_user_type', {}) if per_user_type_mode else {}
        
        setup_time = time.time() - setup_start_time
        print(f"    🔧 Setup completed in {setup_time:.2f}s")
        
        # Check if dynamic prompt UDF is configured (for tool_eval with client attributes)
        # Note: dynamic_prompt_udf already defined above in LLM function selection
        
        if dynamic_prompt_udf:
            # DYNAMIC PROMPT UDF MODE (e.g., tool_eval with per-message client attributes)
            print(f"    🎯 Dynamic Prompt UDF Mode: Using {dynamic_prompt_udf}")
            
            # Count pending records
            count_query = f"""
            SELECT COUNT(*) as pending_count
            FROM {table_name}
            WHERE PROCESSING_STATUS = 'PENDING'
            AND DEPARTMENT = '{department_name}'
            AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
            """
            count_result = session.sql(count_query).collect()
            pending_count = count_result[0]['PENDING_COUNT'] if count_result else 0
            
            print(f"    📊 Found {pending_count} pending records to process")
            
            if pending_count == 0:
                print(f"    ⚠️  No pending records found")
                return True, 0, 0
            
            # Build batch query with dynamic UDF
            # Uses MESSAGE_ID (not CONVERSATION_ID) and MESSAGE (not CONVERSATION_CONTENT)
            # For tool_eval: Also passes HISTORY to support conversation context
            if prompt_type == 'tool_eval':
                # Tool eval mode: Include conversation history and wrap message in template
                # For Gemini models, wrap the message with specific instructions
                if llm_function == 'gemini_chat_system':
                    message_field = """CONCAT(
                        'the user input is:\\n',
                        MESSAGE,
                        '\\nDO NOT ANSWER THE USER AS IF YOU ARE A CHATBOT, STICK TO THE OUTPUT FORMAT IN THE SYSTEM PROMPT, if you see a repeated message in a new streak, consider it as a new message. There is nothing called a Meta-instructional message, the input you see is the user input we need to judge to determine whether we should classify a tool call as ShouldCall or DoNotCall'
                    )"""
                else:
                    message_field = "MESSAGE"
                
                batch_query = f"""
                WITH batch_processing AS (
                    SELECT 
                        MESSAGE_ID,
                        {llm_function}(
                            {message_field},
                            {dynamic_prompt_udf}(CLIENT_ATTRIBUTES),
                            MODEL_NAME,
                            TEMPERATURE,
                            MAX_TOKENS,
                            '{prompt_config.get('reasoning_effort', 'minimal')}',
                            '{department_name}',
                            'single',
                            NULL,
                            NULL,
                            HISTORY,
                            {response_format_param}
                        ) AS llm_result
                    FROM {table_name}
                    WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                    AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                )
                SELECT 
                    MESSAGE_ID, 
                    llm_result:text::string AS LLM_RESPONSE,
                    llm_result:usage        AS TOKENS_BREAKDOWN
                FROM batch_processing
                """
            else:
                # Regular mode: No history parameter
                batch_query = f"""
                WITH batch_processing AS (
                    SELECT 
                        MESSAGE_ID,
                        {llm_function}(
                            MESSAGE,
                            {dynamic_prompt_udf}(CLIENT_ATTRIBUTES),
                            MODEL_NAME,
                            TEMPERATURE,
                            MAX_TOKENS,
                            '{prompt_config.get('reasoning_effort', 'minimal')}',
                            '{department_name}',
                            'single',
                            NULL,
                            NULL,
                            NULL,
                            {response_format_param}
                        ) AS llm_result
                    FROM {table_name}
                    WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                    AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                )
                SELECT 
                    MESSAGE_ID, 
                    llm_result:text::string AS LLM_RESPONSE,
                    llm_result:usage        AS TOKENS_BREAKDOWN
                FROM batch_processing
                """
            
            print(f"    🚀 Running batch with dynamic prompt UDF...")
            execution_start_time = time.time()
            
            try:
                # Execute batch processing
                batch_results = session.sql(batch_query).collect()
                
                if not batch_results:
                    print(f"    ⚠️  No results from batch processing")
                    return True, 0, 0
                
                # Update original table with batch results
                print(f"    🔄 Updating {len(batch_results)} records with LLM responses...")
                
                # Create temp table for efficient UPDATE
                temp_results_table = f"TEMP_UDF_RESULTS_{int(time.time())}"
                
                results_data = [
                    {
                        'MESSAGE_ID': row['MESSAGE_ID'],
                        'LLM_RESPONSE': row['LLM_RESPONSE'],
                        'TOKENS_BREAKDOWN': row['TOKENS_BREAKDOWN']
                    }
                    for row in batch_results
                ]
                results_df = session.create_dataframe(results_data)
                results_df.write.mode("overwrite").save_as_table(temp_results_table)
                
                # Update using MESSAGE_ID join
                update_query = f"""
                UPDATE {table_name}
                SET 
                    LLM_RESPONSE = temp.LLM_RESPONSE,
                    TOKENS_BREAKDOWN = temp.TOKENS_BREAKDOWN,
                    PROCESSING_STATUS = 'COMPLETED'
                FROM {temp_results_table} temp
                WHERE {table_name}.MESSAGE_ID = temp.MESSAGE_ID
                AND {table_name}.DEPARTMENT = '{department_name}'
                """
                
                session.sql(update_query).collect()
                
                # Clean up temp table
                session.sql(f"DROP TABLE {temp_results_table}").collect()
                
                execution_time = time.time() - execution_start_time
                print(f"    ✅ Dynamic UDF batch completed in {execution_time:.2f}s")
                
                # Count results
                processed_count, failed_count = count_llm_results(session, table_name, department_name, target_date)
                
                total_time = time.time() - total_start_time
                print(f"    ⏱️  Total processing time: {total_time:.2f}s")
                
                return True, processed_count, failed_count
                
            except Exception as e:
                print(f"    ❌ Dynamic UDF batch processing failed: {str(e)}")
                traceback.print_exc()
                return False, 0, 0
        
        elif prompt_type == 'tool_eval':
            # STATIC PROMPT MODE FOR TOOL_EVAL (MV_Resolvers)
            # No dynamic UDF - use static prompt with @Prompt@ replacement
            print(f"    🎯 Static Prompt Synchronous Mode (MV_Resolvers tool_eval)")
            
            # Count pending records
            count_query = f"""
            SELECT COUNT(*) as pending_count
            FROM {table_name}
            WHERE PROCESSING_STATUS = 'PENDING'
            AND DEPARTMENT = '{department_name}'
            AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
            """
            count_result = session.sql(count_query).collect()
            pending_count = count_result[0]['PENDING_COUNT'] if count_result else 0
            
            print(f"    📊 Found {pending_count} pending records to process")
            
            if pending_count == 0:
                print(f"    ⚠️  No pending records found")
                return True, 0, 0
            
            print(f"    🚀 Running static prompt batch processing...")
            execution_start_time = time.time()
            
            try:
                # For Gemini models in tool_eval, wrap the message with specific instructions
                if llm_function == 'gemini_chat_system':
                    message_field = """CONCAT(
                        'the user input is:\\n',
                        MESSAGE,
                        '\\nDO NOT ANSWER THE USER AS IF YOU ARE A CHATBOT, STICK TO THE OUTPUT FORMAT IN THE SYSTEM PROMPT, if you see a repeated message in a new streak, consider it as a new message. There is nothing called a Meta-instructional message, the input you see is the user input we need to judge to determine whether we should classify a tool call as ShouldCall or DoNotCall'
                    )"""
                else:
                    message_field = "MESSAGE"
                
                # Build batch query with static prompt and @Prompt@ replacement
                batch_query = f"""
                WITH batch_processing AS (
                    SELECT 
                        MESSAGE_ID,
                        {llm_function}(
                            {message_field},
                            REPLACE($${system_text}$$, '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')),                            MODEL_NAME,
                            TEMPERATURE,
                            MAX_TOKENS,
                            '{prompt_config.get('reasoning_effort', 'minimal')}',
                            '{department_name}',
                            'single',
                            NULL,
                            NULL,
                            NULL,
                            {response_format_param}
                        ) AS llm_result
                    FROM {table_name}
                    WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                    AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                )
                SELECT 
                    MESSAGE_ID, 
                    llm_result:text::string AS LLM_RESPONSE,
                    llm_result:usage        AS TOKENS_BREAKDOWN
                FROM batch_processing
                """
                
                # Execute batch processing
                batch_results = session.sql(batch_query).collect()
                
                if not batch_results:
                    print(f"    ⚠️  No results from batch processing")
                    return True, 0, 0
                
                # Update original table with batch results
                print(f"    🔄 Updating {len(batch_results)} records with LLM responses...")
                
                # Create temp table for efficient UPDATE
                temp_results_table = f"TEMP_STATIC_TOOL_EVAL_RESULTS_{int(time.time())}"
                
                results_data = [
                    {
                        'MESSAGE_ID': row['MESSAGE_ID'],
                        'LLM_RESPONSE': row['LLM_RESPONSE'],
                        'TOKENS_BREAKDOWN': row['TOKENS_BREAKDOWN']
                    }
                    for row in batch_results
                ]
                results_df = session.create_dataframe(results_data)
                results_df.write.mode("overwrite").save_as_table(temp_results_table)
                
                # Update using MESSAGE_ID join
                update_query = f"""
                UPDATE {table_name}
                SET 
                    LLM_RESPONSE = temp.LLM_RESPONSE,
                    TOKENS_BREAKDOWN = temp.TOKENS_BREAKDOWN,
                    PROCESSING_STATUS = 'COMPLETED'
                FROM {temp_results_table} temp
                WHERE {table_name}.MESSAGE_ID = temp.MESSAGE_ID
                AND {table_name}.DEPARTMENT = '{department_name}'
                """
                
                session.sql(update_query).collect()
                
                # Clean up temp table
                session.sql(f"DROP TABLE {temp_results_table}").collect()
                
                execution_time = time.time() - execution_start_time
                print(f"    ✅ Static prompt batch completed in {execution_time:.2f}s")
                
                # Count results (use MESSAGE_ID-based counting for tool_eval)
                success_query = f"""
                SELECT COUNT(*) as count
                FROM {table_name}
                WHERE PROCESSING_STATUS = 'COMPLETED'
                AND DEPARTMENT = '{department_name}'
                AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                AND LLM_RESPONSE IS NOT NULL
                AND LLM_RESPONSE != ''
                """
                failed_query = f"""
                SELECT COUNT(*) as count
                FROM {table_name}
                WHERE PROCESSING_STATUS = 'COMPLETED'
                AND DEPARTMENT = '{department_name}'
                AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                AND (LLM_RESPONSE IS NULL OR LLM_RESPONSE = '')
                """
                
                processed_count = session.sql(success_query).collect()[0]['COUNT']
                failed_count = session.sql(failed_query).collect()[0]['COUNT']
                
                total_time = time.time() - total_start_time
                print(f"    ⏱️  Total processing time: {total_time:.2f}s")
                
                return True, processed_count, failed_count
                
            except Exception as e:
                print(f"    ❌ Static prompt batch processing failed: {str(e)}")
                traceback.print_exc()
                return False, 0, 0
        
        # Step 2: Check pending records count (performance insight)
        count_start_time = time.time()
        
        if per_skill_mode:
            allowed_skills = [str(s).upper() for s in system_text.keys()]
            skill_list_sql = ", ".join(["'" + s.replace("'", "''") + "'" for s in allowed_skills])
            count_query = f"""
            SELECT COUNT(*) as pending_count
            FROM {table_name}
            WHERE PROCESSING_STATUS = 'PENDING'
            AND DEPARTMENT = '{department_name}'
            AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
            AND UPPER(LAST_SKILL) IN ({skill_list_sql})
            """
        else:
            count_query = f"""
            SELECT COUNT(*) as pending_count
            FROM {table_name}
            WHERE PROCESSING_STATUS = 'PENDING'
            AND DEPARTMENT = '{department_name}'
            AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
            """
        
        count_result = session.sql(count_query).collect()
        pending_count = count_result[0]['PENDING_COUNT'] if count_result else 0
        
        count_time = time.time() - count_start_time
        print(f"    📊 Found {pending_count} pending records to process (checked in {count_time:.2f}s)")
        
        if pending_count == 0:
            print(f"    ⚠️  No pending records found for {department_name}")
            return True, 0, 0
        
        print(f"    🚀 Running batch {model_type.upper()} analysis on {pending_count} records...")
        
        # Step 3: Build and execute the UPDATE query
        query_build_start_time = time.time()
        
        # BATCH SQL APPROACH (5x faster than UPDATE)
        query_build_time = time.time() - query_build_start_time
        print(f"    📝 Batch SQL approach selected (5x faster)")
        
        execution_start_time = time.time()
        print(f"    ⏳ Starting LLM batch execution... (estimated: {pending_count * 0.4}s+ for {pending_count} records)")
        batch_sql_completed = False
        
        try:
            # Wrap CONVERSATION_CONTENT for OpenAI when using JSON format (exclude tool_eval)
            print(f"    🔍 JSON wrapping check (update): response_format={response_format}, model_type={model_type}, prompt_type={prompt_type}")
            if response_format == 'json_object' and model_type == 'openai' and prompt_type != 'tool_eval':
                print(f"    ✅ Wrapping CONVERSATION_CONTENT for OpenAI when using JSON format (exclude tool_eval)")
                content_field = "CONCAT('Return your response as JSON.\\n\\n', CONVERSATION_CONTENT)"
            else:
                print(f"    ⏭️  Skipping JSON prefix (response_format={response_format}, model_type={model_type}, prompt_type={prompt_type})")
                content_field = "CONVERSATION_CONTENT"
            
            # Step 1: Check if system prompt contains @Prompt@ placeholder
            needs_prompt_replacement = '@Prompt@' in (system_text if not per_skill_mode else "".join(system_text.values())) if not per_user_type_mode else '@Prompt@' in "".join(system_text_by_user_type.values())
            
            if needs_prompt_replacement or per_skill_mode or per_user_type_mode:
                print(f"    🔄 System prompt contains @Prompt@ - fetching conversation-specific prompts...")
                
                # Create batch processing query with dynamic system prompt replacement
                if per_skill_mode:
                    # Build CASE expression selecting prompt by LAST_SKILL
                    departments_config_full = get_snowflake_llm_departments_config()
                    # allowed skills based on keys
                    allowed_skills = [str(s).upper() for s in system_text.keys()]
                    skill_list_sql = ", ".join(["'" + s.replace("'", "''") + "'" for s in allowed_skills])

                    case_branches = []
                    for skill, prompt in system_text.items():
                        safe_skill = str(skill).upper().replace("'", "''")
                        safe_prompt = prompt  # will be dollar-quoted
                        case_branches.append(f"WHEN UPPER(LAST_SKILL) = '{safe_skill}' THEN $${safe_prompt}$$")
                    case_expr = "CASE " + " ".join(case_branches) + " END"

                    batch_query = f"""
                    WITH batch_processing AS (
                        SELECT 
                            CONVERSATION_ID,
                            {llm_function}(
                                {content_field},
                                REPLACE(
                                    REPLACE({case_expr}, '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')),
                                    '<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>')
                                ),
                                MODEL_NAME,
                                TEMPERATURE,
                                MAX_TOKENS,
                                '{prompt_config.get('reasoning_effort', 'minimal')}',
                                '{department_name}',
                                'single',
                                NULL,
                                NULL,
                                NULL,
                                {response_format_param}
                            ) AS llm_result
                        FROM {table_name}
                        WHERE PROCESSING_STATUS = 'PENDING'
                        AND DEPARTMENT = '{department_name}'
                        AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                        AND UPPER(LAST_SKILL) IN ({skill_list_sql})
                    )
                    SELECT 
                        CONVERSATION_ID, 
                        llm_result:text::string AS LLM_RESPONSE,
                        llm_result:usage        AS TOKENS_BREAKDOWN
                    FROM batch_processing
                    """
                    print(f"    📝 [per_skill] SQL content_field value: {content_field}")
                    print(f"    📝 [per_skill] SQL Preview (first 500 chars): {batch_query[:500]}...")
                elif per_user_type_mode:
                    # Build CASE expression selecting prompt by USER_TYPE
                    case_branches = []
                    for user_type, prompt in system_text_by_user_type.items():
                        safe_user_type = str(user_type).lower().replace("'", "''")
                        safe_prompt = prompt  # will be dollar-quoted
                        case_branches.append(f"WHEN LOWER(USER_TYPE) = '{safe_user_type}' THEN $${safe_prompt}$$")
                    # Add fallback to default client prompt
                    case_branches.append(f"ELSE $${system_text_by_user_type.get('client', system_text)}$$")
                    case_expr = "CASE " + " ".join(case_branches) + " END"

                    batch_query = f"""
                    WITH batch_processing AS (
                        SELECT 
                            CONVERSATION_ID,
                            {llm_function}(
                                {content_field},
                                REPLACE(
                                    REPLACE({case_expr}, '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')),
                                    '<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>')
                                ),
                                MODEL_NAME,
                                TEMPERATURE,
                                MAX_TOKENS,
                                '{prompt_config.get('reasoning_effort', 'minimal')}',
                                '{department_name}',
                                'single',
                                NULL,
                                NULL,
                                NULL,
                                {response_format_param}
                            ) AS llm_result
                        FROM {table_name}
                        WHERE PROCESSING_STATUS = 'PENDING'
                        AND DEPARTMENT = '{department_name}'
                        AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                    )
                    SELECT 
                        CONVERSATION_ID, 
                        llm_result:text::string AS LLM_RESPONSE,
                        llm_result:usage        AS TOKENS_BREAKDOWN
                    FROM batch_processing
                    """
                else:
                    # Check if this is a metrics prompt that needs ERP variable replacement (African or Filipina)
                    filipina_departments = ['AT_Filipina_In_PHL', 'AT_Filipina_Outside_UAE', 'AT_Filipina_Inside_UAE']
                    is_african = department_name == 'AT_African'
                    is_filipina = department_name in filipina_departments
                    needs_erp_vars = prompt_type in ['flow_order', 'profile_update'] and (is_african or is_filipina)
                    
                    # Build ERP CTE if needed
                    if needs_erp_vars:
                        erp_cte = f"""erp_data AS (
                    SELECT 
                        *,
                        GET_N8N_CLIENT_ATTRS_AND_HISTORY(EXECUTION_ID, '{department_name}') AS n8n_data
                    FROM {table_name}
                    WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                    AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                ),
                """
                        from_source = "erp_data"
                    else:
                        erp_cte = ""
                        from_source = table_name
                    
                    # Build prompt_expr based on needs
                    if needs_erp_vars:
                        if is_african:
                            if prompt_type == 'flow_order':
                                # African flow order variables: @photo, @passport, @gcc, @mfa, @attestation, @interviewstatus
                                prompt_expr = f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($${system_text}$$, "
                                prompt_expr += "'@photo', COALESCE(n8n_data:client_attributes:hasFacePhoto::VARCHAR, 'false')), "
                                prompt_expr += "'@passport', COALESCE(n8n_data:client_attributes:hasPassport::VARCHAR, 'false')), "
                                prompt_expr += "'@gcc', COALESCE(n8n_data:client_attributes:hasGCC::VARCHAR, 'false')), "
                                prompt_expr += "'@mfa', COALESCE(n8n_data:client_attributes:hasMfa::VARCHAR, 'false')), "
                                prompt_expr += "'@attestation', COALESCE(n8n_data:client_attributes:hasAttestedGCC::VARCHAR, 'false')), "
                                prompt_expr += "'@interviewstatus', COALESCE(n8n_data:client_attributes:interviewStatus::VARCHAR, 'false')), "
                                prompt_expr += f"'@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')), "
                                prompt_expr += "'<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>'))"
                            else:  # profile_update
                                # African profile update variables: @outcome, @photo, @passport, @gcc, @mfa, @attestation
                                prompt_expr = f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($${system_text}$$, "
                                prompt_expr += "'@outcome', COALESCE(n8n_data:client_attributes:outcome::VARCHAR, 'Unknown')), "
                                prompt_expr += "'@photo', COALESCE(n8n_data:client_attributes:hasFacePhoto::VARCHAR, 'False')), "
                                prompt_expr += "'@passport', COALESCE(n8n_data:client_attributes:hasPassport::VARCHAR, 'False')), "
                                prompt_expr += "'@gcc', COALESCE(n8n_data:client_attributes:hasGCC::VARCHAR, 'False')), "
                                prompt_expr += "'@mfa', COALESCE(n8n_data:client_attributes:hasMfa::VARCHAR, 'False')), "
                                prompt_expr += "'@attestation', COALESCE(n8n_data:client_attributes:hasAttestedGCC::VARCHAR, 'False')), "
                                prompt_expr += f"'@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')), "
                                prompt_expr += "'<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>'))"
                        elif is_filipina:
                            if prompt_type == 'flow_order':
                                # Filipina flow order variables: OEC_Country_Provided, Active_Visa_Provided, Joining_Date_Provided, Passport_Provided, FacePhoto_Provided, OEC_Document_Provided
                                prompt_expr = f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($${system_text}$$, "
                                prompt_expr += "'@photo', COALESCE(n8n_data:client_attributes:hasFacePhoto::VARCHAR, 'false')), "
                                prompt_expr += "'@passport', COALESCE(n8n_data:client_attributes:hasPassport::VARCHAR, 'false')), "
                                prompt_expr += "'@oec', COALESCE(n8n_data:client_attributes:maid_oec_country::VARCHAR, 'false')), "
                                prompt_expr += "'@visa', COALESCE(n8n_data:client_attributes:WorkVisaDocStatus::VARCHAR, 'false')), "
                                prompt_expr += "'@joiningdate', COALESCE(n8n_data:client_attributes:ExpectedDateToJoin::VARCHAR, 'false')), "
                                prompt_expr += "'@oecdoc', COALESCE(n8n_data:client_attributes:maid_oec_country::VARCHAR, 'false')), "
                                prompt_expr += f"'@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')), "
                                prompt_expr += "'<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>'))"
                            else:  # profile_update
                                # Filipina profile update variables: OEC_Country_Provided, application_type, Email, tags_recorded, Joining_date, HasVisa, HasPassport, HasPhoto, HasOec
                                prompt_expr = f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($${system_text}$$, "
                                prompt_expr += "'@oec', COALESCE(n8n_data:client_attributes:maid_oec_country::VARCHAR, 'Unknown')), "
                                prompt_expr += "'@applicationtype', COALESCE(n8n_data:client_attributes:Application_Type::VARCHAR, 'Unknown')), "
                                prompt_expr += "'@email', COALESCE(n8n_data:client_attributes:applicant_email::VARCHAR, 'Unknown')), "
                                prompt_expr += "'@tags', COALESCE(n8n_data:client_attributes:tags_recorded::VARCHAR, 'Unknown')), "
                                prompt_expr += "'@joiningdate', COALESCE(n8n_data:client_attributes:ExpectedDateToJoin::VARCHAR, 'Unknown')), "
                                prompt_expr += "'@visa', COALESCE(n8n_data:client_attributes:WorkVisaDocStatus::VARCHAR, 'False')), "
                                prompt_expr += "'@passport', COALESCE(n8n_data:client_attributes:MaidPassportDocStatus::VARCHAR, 'False')), "
                                prompt_expr += "'@photo', COALESCE(n8n_data:client_attributes:MaidFacePhotoDocStatus::VARCHAR, 'False')), "
                                prompt_expr += "'@oecdoc', COALESCE(n8n_data:client_attributes:maid_oec_country::VARCHAR, 'False')), "
                                prompt_expr += f"'@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')), "
                                prompt_expr += "'<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>'))"
                    else:
                        # Standard processing without ERP variables
                        # ✅ FIX: Use f-string to properly embed system_text value
                        prompt_expr = f"REPLACE(REPLACE($${system_text}$$, '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')), '<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>'))"
                    
                    # Build batch query (with or without erp_cte)
                    batch_query = f"""
                WITH {erp_cte}batch_processing AS (
                    SELECT 
                        CONVERSATION_ID,
                        {llm_function}(
                            {content_field},
                            {prompt_expr},
                            MODEL_NAME,
                            TEMPERATURE,
                            MAX_TOKENS,
                            '{prompt_config.get('reasoning_effort', 'minimal')}',
                            '{department_name}',
                            'single',
                            NULL,
                            NULL,
                            NULL,
                            {response_format_param}
                        ) AS llm_result
                    FROM {from_source}
                    WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                        AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                )
                SELECT 
                    CONVERSATION_ID, 
                    llm_result:text::string AS LLM_RESPONSE,
                    llm_result:usage        AS TOKENS_BREAKDOWN
                FROM batch_processing
                """
            else:
                # Original batch processing without prompt replacement
                if per_skill_mode:
                    allowed_skills = [str(system_text.keys())]
                    skill_list_sql = ", ".join(["'" + str(s).upper().replace("'", "''") + "'" for s in system_text.keys()])
                    case_branches = []
                    for skill, prompt in system_text.items():
                        safe_skill = str(skill).upper().replace("'", "''")
                        safe_prompt = prompt
                        case_branches.append(f"WHEN UPPER(LAST_SKILL) = '{safe_skill}' THEN $${safe_prompt}$$")
                    case_expr = "CASE " + " ".join(case_branches) + " END"
                    batch_query = f"""
                    WITH batch_processing AS (
                        SELECT 
                            CONVERSATION_ID,
                            {llm_function}(
                                {content_field},
                                {case_expr},
                                MODEL_NAME,
                                TEMPERATURE,
                                MAX_TOKENS,
                                '{prompt_config.get('reasoning_effort', 'minimal')}',
                                '{department_name}',
                                'single',
                                NULL,
                                NULL,
                                NULL,
                                {response_format_param}
                            ) AS llm_result
                        FROM {table_name}
                        WHERE PROCESSING_STATUS = 'PENDING'
                        AND DEPARTMENT = '{department_name}'
                        AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                        AND UPPER(LAST_SKILL) IN ({skill_list_sql})
                    )
                    SELECT 
                        CONVERSATION_ID, 
                        llm_result:text::string AS LLM_RESPONSE,
                        llm_result:usage        AS TOKENS_BREAKDOWN
                    FROM batch_processing
                    """
                elif per_user_type_mode:
                    # Build CASE statement for user type-based prompts
                    case_branches = []
                    for user_type, prompt in system_text_by_user_type.items():
                        safe_user_type = str(user_type).upper().replace("'", "''")
                        safe_prompt = prompt
                        case_branches.append(f"WHEN UPPER(USER_TYPE) = '{safe_user_type}' THEN $${safe_prompt}$$")
                    # Add default case (client prompt)
                    default_prompt = system_text_by_user_type.get('client', system_text)
                    case_branches.append(f"ELSE $${default_prompt}$$")
                    case_expr = "CASE " + " ".join(case_branches) + " END"
                    
                    batch_query = f"""
                WITH batch_processing AS (
                    SELECT 
                        CONVERSATION_ID,
                        {llm_function}(
                            {content_field},
                            {case_expr},
                            MODEL_NAME,
                            TEMPERATURE,
                            MAX_TOKENS,
                            '{prompt_config.get('reasoning_effort', 'minimal')}',
                            '{department_name}',
                            'single',
                            NULL,
                            NULL,
                            NULL,
                            {response_format_param}
                        ) AS llm_result
                    FROM {table_name}
                    WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                    AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                )
                SELECT 
                    CONVERSATION_ID, 
                    llm_result:text::string AS LLM_RESPONSE,
                    llm_result:usage        AS TOKENS_BREAKDOWN
                FROM batch_processing
                """
                else:
                    # Check if this is a metrics prompt that needs ERP variable replacement (African or Filipina)
                    filipina_departments = ['AT_Filipina_In_PHL', 'AT_Filipina_Outside_UAE', 'AT_Filipina_Inside_UAE']
                    is_african = department_name == 'AT_African'
                    is_filipina = department_name in filipina_departments
                    needs_erp_vars = prompt_type in ['flow_order', 'profile_update'] and (is_african or is_filipina)
                    
                    # Build ERP CTE if needed
                    if needs_erp_vars:
                        erp_cte = f"""erp_data AS (
                    SELECT 
                        *,
                        GET_N8N_CLIENT_ATTRS_AND_HISTORY(EXECUTION_ID, '{department_name}') AS n8n_data
                    FROM {table_name}
                    WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                    AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                ),
                """
                        from_source = "erp_data"
                    else:
                        erp_cte = ""
                        from_source = table_name
                    
                    # Build prompt_expr based on needs
                    if needs_erp_vars:
                        if is_african:
                            if prompt_type == 'flow_order':
                                # African flow order variables: @photo, @passport, @gcc, @mfa, @attestation, @interviewstatus
                                prompt_expr = f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($${system_text}$$, "
                                prompt_expr += "'@photo', COALESCE(n8n_data:client_attributes:hasFacePhoto::VARCHAR, 'false')), "
                                prompt_expr += "'@passport', COALESCE(n8n_data:client_attributes:hasPassport::VARCHAR, 'false')), "
                                prompt_expr += "'@gcc', COALESCE(n8n_data:client_attributes:hasGCC::VARCHAR, 'false')), "
                                prompt_expr += "'@mfa', COALESCE(n8n_data:client_attributes:hasMfa::VARCHAR, 'false')), "
                                prompt_expr += "'@attestation', COALESCE(n8n_data:client_attributes:hasAttestedGCC::VARCHAR, 'false')), "
                                prompt_expr += "'@interviewstatus', COALESCE(n8n_data:client_attributes:interviewStatus::VARCHAR, 'false')), "
                                prompt_expr += f"'@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')), "
                                prompt_expr += "'<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>'))"
                            else:  # profile_update
                                # African profile update variables: @outcome, @photo, @passport, @gcc, @mfa, @attestation
                                prompt_expr = f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($${system_text}$$, "
                                prompt_expr += "'@outcome', COALESCE(n8n_data:client_attributes:outcome::VARCHAR, 'Unknown')), "
                                prompt_expr += "'@photo', COALESCE(n8n_data:client_attributes:hasFacePhoto::VARCHAR, 'False')), "
                                prompt_expr += "'@passport', COALESCE(n8n_data:client_attributes:hasPassport::VARCHAR, 'False')), "
                                prompt_expr += "'@gcc', COALESCE(n8n_data:client_attributes:hasGCC::VARCHAR, 'False')), "
                                prompt_expr += "'@mfa', COALESCE(n8n_data:client_attributes:hasMfa::VARCHAR, 'False')), "
                                prompt_expr += "'@attestation', COALESCE(n8n_data:client_attributes:hasAttestedGCC::VARCHAR, 'False')), "
                                prompt_expr += f"'@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')), "
                                prompt_expr += "'<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>'))"
                        elif is_filipina:
                            if prompt_type == 'flow_order':
                                # Filipina flow order variables: OEC_Country_Provided, Active_Visa_Provided, Joining_Date_Provided, Passport_Provided, FacePhoto_Provided, OEC_Document_Provided
                                prompt_expr = f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($${system_text}$$, "
                                prompt_expr += "'@photo', COALESCE(n8n_data:client_attributes:hasFacePhoto::VARCHAR, 'false')), "
                                prompt_expr += "'@passport', COALESCE(n8n_data:client_attributes:hasPassport::VARCHAR, 'false')), "
                                prompt_expr += "'@oec', COALESCE(n8n_data:client_attributes:maid_oec_country::VARCHAR, 'false')), "
                                prompt_expr += "'@visa', COALESCE(n8n_data:client_attributes:WorkVisaDocStatus::VARCHAR, 'false')), "
                                prompt_expr += "'@joiningdate', COALESCE(n8n_data:client_attributes:ExpectedDateToJoin::VARCHAR, 'false')), "
                                prompt_expr += "'@oecdoc', COALESCE(n8n_data:client_attributes:maid_oec_country::VARCHAR, 'false')), "
                                prompt_expr += f"'@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')), "
                                prompt_expr += "'<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>'))"
                            else:  # profile_update
                                # Filipina profile update variables: OEC_Country_Provided, application_type, Email, tags_recorded, Joining_date, HasVisa, HasPassport, HasPhoto, HasOec
                                prompt_expr = f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($${system_text}$$, "
                                prompt_expr += "'@oec', COALESCE(n8n_data:client_attributes:maid_oec_country::VARCHAR, 'Unknown')), "
                                prompt_expr += "'@applicationtype', COALESCE(n8n_data:client_attributes:Application_Type::VARCHAR, 'Unknown')), "
                                prompt_expr += "'@email', COALESCE(n8n_data:client_attributes:applicant_email::VARCHAR, 'Unknown')), "
                                prompt_expr += "'@tags', COALESCE(n8n_data:client_attributes:tags_recorded::VARCHAR, 'Unknown')), "
                                prompt_expr += "'@joiningdate', COALESCE(n8n_data:client_attributes:ExpectedDateToJoin::VARCHAR, 'Unknown')), "
                                prompt_expr += "'@visa', COALESCE(n8n_data:client_attributes:WorkVisaDocStatus::VARCHAR, 'False')), "
                                prompt_expr += "'@passport', COALESCE(n8n_data:client_attributes:MaidPassportDocStatus::VARCHAR, 'False')), "
                                prompt_expr += "'@photo', COALESCE(n8n_data:client_attributes:MaidFacePhotoDocStatus::VARCHAR, 'False')), "
                                prompt_expr += "'@oecdoc', COALESCE(n8n_data:client_attributes:maid_oec_country::VARCHAR, 'False')), "
                                prompt_expr += f"'@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')), "
                                prompt_expr += "'<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>'))"
                    else:
                        # Standard processing without Kenyan ERP variables
                        # ✅ FIX: Use f-string to properly embed system_text value
                        prompt_expr = f"REPLACE(REPLACE($${system_text}$$, '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')), '<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>'))"
                    
                    # 🔍 DEBUG: Print system_text length to verify it's not empty
                    print(f"    🐛 DEBUG: system_text length = {len(system_text) if system_text else 0} characters")
                    print(f"    🐛 DEBUG: First 200 chars of system_text: {system_text[:200] if system_text else '(EMPTY!)'}")
                    
                    # Build batch query (with or without erp_cte)
                    batch_query = f"""
                WITH {erp_cte}batch_processing AS (
                    SELECT 
                        CONVERSATION_ID,
                        {llm_function}(
                            {content_field},
                            {prompt_expr},
                            MODEL_NAME,
                            TEMPERATURE,
                            MAX_TOKENS,
                            '{prompt_config.get('reasoning_effort', 'minimal')}',
                            '{department_name}',
                            'single',
                            NULL,
                            NULL,
                            NULL,
                            {response_format_param}
                        ) AS llm_result
                    FROM {from_source}
                    WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                        AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                )
                SELECT 
                    CONVERSATION_ID, 
                    llm_result:text::string AS LLM_RESPONSE,
                    llm_result:usage        AS TOKENS_BREAKDOWN
                FROM batch_processing
                """
            print(f"\n{'='*80}")
            print(f"🐛 DEBUG: Generated SQL Query")
            print(f"{'='*80}")
            # Only print first 2000 chars to avoid overwhelming output
            print(batch_query[:2000] + "..." if len(batch_query) > 2000 else batch_query)
            print(f"{'='*80}\n")
            
            # Execute batch processing
            batch_results = session.sql(batch_query).collect()
        
            if not batch_results:
                # If the batch query returns nothing, fallback to the UPDATE method below
                raise RuntimeError("No results from batch processing")
            else:
                # Step 2: Update original table with batch results
                print(f"    🔄 Updating {len(batch_results)} records with LLM responses...")
                
                # Create temp table with results for efficient UPDATE
                temp_results_table = f"TEMP_BATCH_RESULTS_{int(time.time())}"
                
                # Convert results to DataFrame
                results_data = [
                    {
                        'CONVERSATION_ID': row['CONVERSATION_ID'],
                        'LLM_RESPONSE': row['LLM_RESPONSE'],
                        'TOKENS_BREAKDOWN': row['TOKENS_BREAKDOWN']
                    }
                    for row in batch_results
                ]
                results_df = session.create_dataframe(results_data)
                results_df.write.mode("overwrite").save_as_table(temp_results_table)
                
                # Update original table using JOIN on CONVERSATION_ID
                update_query = f"""
                UPDATE {table_name}
                SET 
                    LLM_RESPONSE = temp.LLM_RESPONSE,
                    TOKENS_BREAKDOWN = temp.TOKENS_BREAKDOWN,
                    PROCESSING_STATUS = 'COMPLETED'
                FROM {temp_results_table} temp
                WHERE {table_name}.CONVERSATION_ID = temp.CONVERSATION_ID
                AND {table_name}.DEPARTMENT = '{department_name}'
                """
                
                session.sql(update_query).collect()
                
                # Clean up temp table
                session.sql(f"DROP TABLE {temp_results_table}").collect()
                
                execution_time = time.time() - execution_start_time
                print(f"    ✅ Batch SQL approach completed successfully")
                batch_sql_completed = True
                
        except Exception as batch_error:
            print(f"    ⚠️  Batch SQL failed, falling back to UPDATE method: {str(batch_error)}")
            
            # Fallback to original UPDATE method with prompt replacement support
            if needs_prompt_replacement or per_skill_mode:
                print(f"    🔄 Using UPDATE method with @Prompt@ replacement...")
                if per_skill_mode:
                    allowed_skills = [str(s).upper() for s in system_text.keys()]
                    skill_list_sql = ", ".join(["'" + s.replace("'", "''") + "'" for s in allowed_skills])
                    case_branches = []
                    for skill, prompt in system_text.items():
                        safe_skill = str(skill).upper().replace("'", "''")
                        safe_prompt = prompt
                        case_branches.append(
                            f"WHEN UPPER(LAST_SKILL) = '{safe_skill}' THEN REPLACE($${safe_prompt}$$, '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT({table_name}.EXECUTION_ID, '{department_name}'), '@Prompt@'))"
                        )
                    case_expr = "CASE " + " ".join(case_branches) + " END"
                    update_query = f"""
                    UPDATE {table_name}
                    SET 
                        LLM_RESPONSE = (
                            {llm_function}(
                                CONVERSATION_CONTENT,
                                REPLACE({case_expr}, '<STEP-NAME>', COALESCE({table_name}.LAST_SKILL, '<STEP-NAME>')),
                                MODEL_NAME,
                                TEMPERATURE,
                                MAX_TOKENS,
                                '{prompt_config.get('reasoning_effort', 'minimal')}',
                                '{department_name}'
                            ):text::string
                        ),
                        TOKENS_BREAKDOWN = '',
                        PROCESSING_STATUS = 'COMPLETED'
                    WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                    AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                    AND LAST_SKILL IN ({skill_list_sql})
                    """
                else:
                    update_query = f"""
                    UPDATE {table_name}
                    SET 
                        LLM_RESPONSE = (
                            {llm_function}(
                                CONVERSATION_CONTENT,
                                REPLACE(
                                    REPLACE($${system_text}$$, '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT({table_name}.EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT({table_name}.CONVERSATION_ID), '@Prompt@')),
                                    '<STEP-NAME>', COALESCE({table_name}.LAST_SKILL, '<STEP-NAME>')
                                ),
                                MODEL_NAME,
                                TEMPERATURE,
                                MAX_TOKENS,
                                '{prompt_config.get('reasoning_effort', 'minimal')}',
                                '{department_name}'
                            ):text::string
                        ),
                        TOKENS_BREAKDOWN = '',
                        PROCESSING_STATUS = 'COMPLETED'
                    WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                    AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                    """
            else:
                # Original UPDATE method without prompt replacement
                update_query = f"""
                UPDATE {table_name}
                SET 
                    LLM_RESPONSE = (
                        {llm_function}(
                            CONVERSATION_CONTENT,
                            $${system_text}$$,
                            MODEL_NAME,
                            TEMPERATURE,
                            MAX_TOKENS,
                            '{prompt_config.get('reasoning_effort', 'minimal')}',
                            '{department_name}'
                        ):text::string
                    ),
                    TOKENS_BREAKDOWN = '',
                    PROCESSING_STATUS = 'COMPLETED'
                WHERE PROCESSING_STATUS = 'PENDING'
                AND DEPARTMENT = '{department_name}'
                AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
                """

        # If the fast batch SQL path succeeded, we have already updated the table. Do NOT run the fallback UPDATE again.
        if batch_sql_completed:
            counting_start_time = time.time()
            if per_skill_mode:
                allowed_skills = [str(s).upper() for s in system_text.keys()]
                skill_list_sql = ", ".join(["'" + s.replace("'", "''") + "'" for s in allowed_skills])
                processed_count, failed_count = count_llm_results_with_extra_filter(
                    session, table_name, department_name, target_date, f"AND UPPER(LAST_SKILL) IN ({skill_list_sql})"
                )
            else:
                processed_count, failed_count = count_llm_results(session, table_name, department_name, target_date)
            counting_time = time.time() - counting_start_time
            total_time = time.time() - total_start_time
            print(f"    📊 Results counted in {counting_time:.2f}s")
            print(f"    🏁 TOTAL BATCH TIME: {total_time:.2f}s for {pending_count} records")
            return True, processed_count, failed_count
        
        result = session.sql(update_query).collect()
        execution_time = time.time() - execution_start_time
        print(f"    ✅ Fallback UPDATE method completed")
        records_per_second = pending_count / execution_time if execution_time > 0 else 0
        
        print(f"    ✅ Batch UPDATE executed in {execution_time:.2f}s")
        print(f"    📈 Performance: {records_per_second:.2f} records/second")
        print(f"    🔍 Average time per record: {execution_time/pending_count:.2f}s" if pending_count > 0 else "")
        
        # Step 4: Count successes and failures with timing
        counting_start_time = time.time()
        
        # Narrow counting for per-skill mode to the allowed skill subset
        if per_skill_mode:
            allowed_skills = [str(s).upper() for s in system_text.keys()]
            skill_list_sql = ", ".join(["'" + s.replace("'", "''") + "'" for s in allowed_skills])
            processed_count, failed_count = count_llm_results_with_extra_filter(
                session, table_name, department_name, target_date, f"AND UPPER(LAST_SKILL) IN ({skill_list_sql})"
            )
        else:
            processed_count, failed_count = count_llm_results(session, table_name, department_name, target_date)
        
        counting_time = time.time() - counting_start_time
        print(f"    📊 Results counted in {counting_time:.2f}s")
        
        # Total timing summary
        total_time = time.time() - total_start_time
        print(f"    🏁 TOTAL BATCH TIME: {total_time:.2f}s for {pending_count} records")
        print(f"    📋 Time breakdown:")
        print(f"       - Setup: {setup_time:.2f}s ({setup_time/total_time*100:.1f}%)")
        print(f"       - Counting: {count_time:.2f}s ({count_time/total_time*100:.1f}%)")
        print(f"       - Query Build: {query_build_time:.3f}s ({query_build_time/total_time*100:.1f}%)")
        print(f"       - LLM Execution: {execution_time:.2f}s ({execution_time/total_time*100:.1f}%)")
        print(f"       - Results: {counting_time:.2f}s ({counting_time/total_time*100:.1f}%)")
        
        # Performance insights
        if execution_time > 30:
            print(f"    ⚠️  SLOW EXECUTION DETECTED!")
            print(f"       - Consider reducing batch size or checking Snowflake compute resources")
            print(f"       - {model_type.upper()} may be rate-limited or overloaded")
        
        if records_per_second < 0.5:
            print(f"    🐌 LOW THROUGHPUT WARNING: {records_per_second:.2f} records/second")
            print(f"       - Expected: 1-5 records/second for typical LLM operations")
        
        return True, processed_count, failed_count
        
    except Exception as e:
        total_time = time.time() - total_start_time
        error_details = format_error_details(e, f"BATCH LLM UPDATE - {department_name}")
        print(f"    ❌ Batch LLM update failed after {total_time:.2f}s: {str(e)}")
        print(error_details)
        return False, 0, 0


# =============================================================================
# EXTERNAL EXECUTION MODE (GitHub Actions / Colab)
# =============================================================================

def run_batch_llm_external(session: snowpark.Session, prompt_config, department_name, target_date, prompt_type=None):
    """
    Run LLM analysis using external HTTP client (parallel async calls).
    
    This bypasses Snowflake UDFs and makes direct HTTP calls to OpenAI API,
    enabling parallel processing and avoiding Snowflake compute costs during
    API wait time.
    
    REQUIRES: llm_clients module to be installed and OPENAI_API_KEY env var set.
    
    Args:
        session: Snowflake session (used for reading pending records and writing results)
        prompt_config: Prompt configuration dictionary
        department_name: Department name
        target_date: Target date for analysis
        prompt_type: Prompt type (e.g., 'client_suspecting_ai', 'ftr', etc.)
    
    Returns:
        Tuple: (success: bool, processed_count: int, failed_count: int)
    """
    import time
    
    if not is_external_execution_available():
        print(f"    ❌ External execution not available. Use 'synchronous' mode instead.")
        return False, 0, 0
    
    total_start_time = time.time()
    
    try:
        # Get configuration
        table_name = prompt_config['output_table']
        model = prompt_config.get('model', 'gpt-4o-mini')
        system_prompt = prompt_config.get('system_prompt', '')
        temperature = prompt_config.get('temperature', 0)
        max_tokens = prompt_config.get('max_tokens', 2048)
        
        target_date_str = target_date if target_date else datetime.now().strftime("%Y-%m-%d")
        
        # Handle per-skill prompts (e.g., loss_interest)
        per_skill_mode = isinstance(system_prompt, dict)
        if per_skill_mode:
            print(f"    ⚠️  Per-skill prompts not yet supported in external mode. Using first skill's prompt.")
            system_prompt = list(system_prompt.values())[0]
        
        print(f"    🌐 External execution mode: Using llm_clients (parallel HTTP)")
        print(f"    📋 Model: {model}, Max Tokens: {max_tokens}")
        
        # Step 1: Read pending records from Snowflake
        read_start = time.time()
        pending_query = f"""
        SELECT 
            CONVERSATION_ID,
            SEGMENT_ID,
            CONVERSATION_CONTENT
        FROM {table_name}
        WHERE PROCESSING_STATUS = 'PENDING'
        AND DEPARTMENT = '{department_name}'
        AND DATE = '{target_date_str}'
        """
        
        pending_records = session.sql(pending_query).collect()
        pending_count = len(pending_records)
        read_time = time.time() - read_start
        
        print(f"    📊 Found {pending_count} pending records (read in {read_time:.2f}s)")
        
        if pending_count == 0:
            print(f"    ⚠️  No pending records for {department_name}")
            return True, 0, 0
        
        # Step 2: Prepare items for LLM client
        items = []
        for row in pending_records:
            items.append({
                'conversation_id': f"{row['CONVERSATION_ID']}_{row['SEGMENT_ID']}" if row.get('SEGMENT_ID') else row['CONVERSATION_ID'],
                'content': row['CONVERSATION_CONTENT'],
                '_conversation_id': row['CONVERSATION_ID'],
                '_segment_id': row.get('SEGMENT_ID', row['CONVERSATION_ID'])
            })
        
        # Step 3: Get LLM client and process
        client = get_llm_client()
        
        print(f"    🚀 Processing {pending_count} records with parallel LLM calls...")
        llm_start = time.time()
        
        results = client.call_batch(
            items=[{'conversation_id': item['conversation_id'], 'content': item['content']} for item in items],
            system_prompt=system_prompt,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            verbose=True,
            department=department_name
        )
        
        llm_time = time.time() - llm_start
        print(f"    ⏱️  LLM processing completed in {llm_time:.2f}s")
        
        # Step 4: Write results back to Snowflake
        write_start = time.time()
        
        # Map results back to original IDs
        results_by_id = {r.conversation_id: r for r in results}
        
        processed_count = 0
        failed_count = 0
        
        # Create temp table for efficient UPDATE
        temp_table_name = f"TEMP_EXTERNAL_RESULTS_{int(time.time())}"
        
        results_data = []
        for item in items:
            result = results_by_id.get(item['conversation_id'])
            if result and result.success:
                results_data.append({
                    'CONVERSATION_ID': item['_conversation_id'],
                    'SEGMENT_ID': item['_segment_id'],
                    'LLM_RESPONSE': result.response or '',
                    'TOKENS_BREAKDOWN': result.tokens_breakdown,
                    'PROCESSING_STATUS': 'COMPLETED'
                })
                processed_count += 1
            else:
                error_msg = result.error.message if result and result.error else 'Unknown error'
                results_data.append({
                    'CONVERSATION_ID': item['_conversation_id'],
                    'SEGMENT_ID': item['_segment_id'],
                    'LLM_RESPONSE': f'ERROR: {error_msg}',
                    'TOKENS_BREAKDOWN': '{}',
                    'PROCESSING_STATUS': 'FAILED'
                })
                failed_count += 1
        
        # Create temp table and update
        if results_data:
            results_df = session.create_dataframe(results_data)
            results_df.write.mode("overwrite").save_as_table(temp_table_name)
            
            # Update using CONVERSATION_ID + SEGMENT_ID join
            update_query = f"""
            UPDATE {table_name}
            SET 
                LLM_RESPONSE = temp.LLM_RESPONSE,
                TOKENS_BREAKDOWN = PARSE_JSON(temp.TOKENS_BREAKDOWN),
                PROCESSING_STATUS = temp.PROCESSING_STATUS
            FROM {temp_table_name} temp
            WHERE {table_name}.CONVERSATION_ID = temp.CONVERSATION_ID
            AND {table_name}.SEGMENT_ID = temp.SEGMENT_ID
            AND {table_name}.DEPARTMENT = '{department_name}'
            AND {table_name}.DATE = '{target_date_str}'
            """
            
            session.sql(update_query).collect()
            session.sql(f"DROP TABLE IF EXISTS {temp_table_name}").collect()
        
        write_time = time.time() - write_start
        total_time = time.time() - total_start_time
        
        success_rate = (processed_count / pending_count * 100) if pending_count > 0 else 0
        
        print(f"    ✅ External processing complete:")
        print(f"       - Processed: {processed_count}/{pending_count} ({success_rate:.1f}%)")
        print(f"       - Failed: {failed_count}")
        print(f"       - Read time: {read_time:.2f}s")
        print(f"       - LLM time: {llm_time:.2f}s")
        print(f"       - Write time: {write_time:.2f}s")
        print(f"       - Total time: {total_time:.2f}s")
        
        return True, processed_count, failed_count
        
    except Exception as e:
        total_time = time.time() - total_start_time
        print(f"    ❌ External LLM processing failed after {total_time:.2f}s: {str(e)}")
        traceback.print_exc()
        return False, 0, 0


def run_batch_llm_submit(session: snowpark.Session, prompt_config, department_name, target_date, prompt_type=None):
    """
    Submit batch to OpenAI/Gemini Batch API without waiting for completion
    
    This function submits a batch job to OpenAI/Gemini and returns immediately with the batch_id.
    It does NOT wait for the batch to complete.
    
    Args:
        session: Snowflake session
        prompt_config: Prompt configuration dictionary
        department_name: Department name
        target_date: Target date for analysis
        prompt_type: Prompt type (e.g., 'tool_eval', 'SA_prompt', etc.)
    
    Returns:
        Tuple: (success: bool, batch_id: str, input_file_id: str, total_requests: int)
    """
    import time
    
    try:
        # Setup
        table_name = prompt_config['output_table']
        model_type = prompt_config.get('model_type', 'openai').lower()
        system_text = prompt_config.get('system_prompt', '')
        per_skill = isinstance(system_text, dict)
        per_user_type_mode = prompt_config.get('per_user_type_mode', False)  # threatening per USER_TYPE
        system_text_by_user_type = prompt_config.get('system_prompt_by_user_type', {}) if per_user_type_mode else {}
        reasoning_effort = prompt_config.get('reasoning_effort', 'minimal')
        
        # Extract response_format from config
        response_format = prompt_config.get('response_format', None)
        response_format_param = f"'{response_format}'" if response_format else 'NULL'
        
        # Choose the appropriate LLM function based on model_type AND prompt_type
        dynamic_prompt_udf = prompt_config.get('dynamic_prompt_udf')
        
        if model_type == 'openai':
            # For tool_eval, check if dynamic UDF is configured
            if prompt_type == 'tool_eval':
                if dynamic_prompt_udf:
                    # MV_Sales mode: Chat Completions API with dynamic UDF
                    llm_function = 'openai_chat_system_completions'
                    print(f"    🎯 MV_Sales mode: Using Chat Completions API (openai_chat_system_completions)", flush=True)
                else:
                    # MV_Resolvers mode: Responses API with static prompt
                    llm_function = 'openai_chat_system'
                    print(f"    🎯 MV_Resolvers mode: Using Responses API (openai_chat_system)", flush=True)
            else:
                llm_function = 'openai_chat_system'
                print(f"    🎯 Using Responses API (openai_chat_system) for {prompt_type}", flush=True)
        elif model_type == 'gemini':
            llm_function = 'gemini_chat_system'
        else:
            print(f"    ❌ Unsupported model_type: {model_type}")
            return False, None, None, 0
        
        target_date_str = target_date if target_date else datetime.now().strftime("%Y-%m-%d")
        
        print(f"    📤 Submitting batch to {model_type.upper()}...", flush=True)
        
        # Check if dynamic prompt UDF is configured
        dynamic_prompt_udf = prompt_config.get('dynamic_prompt_udf')
        
        if dynamic_prompt_udf:
            # DYNAMIC PROMPT UDF BATCH SUBMIT MODE
            print(f"    🎯 Dynamic Prompt UDF Batch Submit: Using {dynamic_prompt_udf}", flush=True)
            
            # Count pending records
            count_sql = f"""
                SELECT COUNT(*) AS pending_count
                FROM {table_name}
                WHERE PROCESSING_STATUS = 'PENDING'
                  AND DEPARTMENT = '{department_name}'
                  AND DATE = '{target_date_str}'
            """
            pending_count = session.sql(count_sql).collect()[0]['PENDING_COUNT']
            
            if pending_count == 0:
                print(f"    ⚠️  No pending records to submit", flush=True)
                return True, None, None, 0
            
            print(f"    [{datetime.now().strftime('%H:%M:%S')}] 📊 Submitting batch for {pending_count} records...", flush=True)
            
            # Build batch submission SQL with dynamic UDF
            # For tool_eval: Need to include HISTORY in the batch items
            if prompt_type == 'tool_eval':
                # Tool eval mode: Include conversation history in batch items and wrap message
                # NOTE: OpenAI Batch API for Responses endpoint does NOT support history parameter
                # So we need to use Chat Completions batch format OR pass history as part of input
                # For now, we'll handle this in the UDF by using openai_chat_system_completions
                batch_submit_sql = f"""
                    WITH base AS (
                    SELECT
                        MESSAGE_ID,
                        MESSAGE,
                        CLIENT_ATTRIBUTES,
                        HISTORY,
                        MODEL_NAME,
                        TEMPERATURE,
                        MAX_TOKENS
                    FROM {table_name}
                    WHERE PROCESSING_STATUS = 'PENDING'
                        AND DEPARTMENT = '{department_name}'
                        AND DATE = '{target_date_str}'
                    ),
                    prompts AS (
                    SELECT
                        MESSAGE_ID,
                        MESSAGE  AS WRAPPED_MESSAGE,
                        {dynamic_prompt_udf}(CLIENT_ATTRIBUTES) AS INSTRUCTIONS,
                        HISTORY,
                        MODEL_NAME,
                        TEMPERATURE,
                        MAX_TOKENS
                    FROM base
                    ),
                    model_params AS (
                    SELECT
                        ANY_VALUE(MODEL_NAME)    AS MODEL_NAME,
                        ANY_VALUE(TEMPERATURE)   AS TEMPERATURE,
                        ANY_VALUE(MAX_TOKENS)    AS MAX_TOKENS
                    FROM prompts
                    ),
                    items AS (
                    SELECT
                        ARRAY_AGG(
                        OBJECT_CONSTRUCT(
                            'custom_id',    MESSAGE_ID,
                            'input',        WRAPPED_MESSAGE,
                            'instructions', INSTRUCTIONS,
                            'history',      HISTORY
                        )
                        ) WITHIN GROUP (ORDER BY MESSAGE_ID) AS arr
                    FROM prompts
                    ),
                    submit AS (
                    SELECT {llm_function}(
                            NULL,
                            NULL,
                            (SELECT MODEL_NAME  FROM model_params),
                            (SELECT TEMPERATURE FROM model_params),
                            (SELECT MAX_TOKENS  FROM model_params),
                            '{reasoning_effort}',
                            '{department_name}',
                            'batch_submit',
                            TO_JSON(arr),
                            NULL,
                            NULL,
                            {response_format_param}
                            ) AS resp
                    FROM items
                    )
                    SELECT
                    resp:batch_id::string      AS BATCH_ID,
                    resp:status::string        AS BATCH_STATUS,
                    resp:input_file_id::string AS INPUT_FILE_ID
                    FROM submit
                """
            else:
                # Regular mode: No history in batch items
                batch_submit_sql = f"""
                    WITH base AS (
                    SELECT
                        MESSAGE_ID,
                        MESSAGE,
                        CLIENT_ATTRIBUTES,
                        MODEL_NAME,
                        TEMPERATURE,
                        MAX_TOKENS
                    FROM {table_name}
                    WHERE PROCESSING_STATUS = 'PENDING'
                        AND DEPARTMENT = '{department_name}'
                        AND DATE = '{target_date_str}'
                    ),
                    prompts AS (
                    SELECT
                        MESSAGE_ID,
                        MESSAGE,
                        {dynamic_prompt_udf}(CLIENT_ATTRIBUTES) AS INSTRUCTIONS,
                        MODEL_NAME,
                        TEMPERATURE,
                        MAX_TOKENS
                    FROM base
                    ),
                    model_params AS (
                    SELECT
                        ANY_VALUE(MODEL_NAME)    AS MODEL_NAME,
                        ANY_VALUE(TEMPERATURE)   AS TEMPERATURE,
                        ANY_VALUE(MAX_TOKENS)    AS MAX_TOKENS
                    FROM prompts
                    ),
                    items AS (
                    SELECT
                        ARRAY_AGG(
                        OBJECT_CONSTRUCT(
                            'custom_id',    MESSAGE_ID,
                            'input',        MESSAGE,
                            'instructions', INSTRUCTIONS
                        )
                        ) WITHIN GROUP (ORDER BY MESSAGE_ID) AS arr
                    FROM prompts
                    ),
                    submit AS (
                    SELECT {llm_function}(
                            NULL,
                            NULL,
                            (SELECT MODEL_NAME  FROM model_params),
                            (SELECT TEMPERATURE FROM model_params),
                            (SELECT MAX_TOKENS  FROM model_params),
                            '{reasoning_effort}',
                            '{department_name}',
                            'batch_submit',
                            TO_JSON(arr),
                            NULL,
                            NULL,
                            {response_format_param}
                            ) AS resp
                    FROM items
                    )
                    SELECT
                    resp:batch_id::string      AS BATCH_ID,
                    resp:status::string        AS BATCH_STATUS,
                    resp:input_file_id::string AS INPUT_FILE_ID
                    FROM submit
                """
            
            print(f"    [{datetime.now().strftime('%H:%M:%S')}] ⏳ Executing dynamic UDF batch submit SQL...", flush=True)
            
            # Execute batch submit
            result = session.sql(batch_submit_sql).collect()
            
            if not result:
                print(f"    ❌ Batch submission failed - no result", flush=True)
                return False, None, None, 0
            
            batch_id = result[0]['BATCH_ID']
            batch_status = result[0]['BATCH_STATUS']
            input_file_id = result[0]['INPUT_FILE_ID']
            
            print(f"    ✅ Dynamic UDF batch submitted successfully!", flush=True)
            print(f"    📝 Batch ID: {batch_id}", flush=True)
            print(f"    📁 Input File ID: {input_file_id}", flush=True)
            print(f"    📊 Total records: {pending_count}", flush=True)
            
            return True, batch_id, input_file_id, pending_count
        
        elif prompt_type == 'tool_eval':
            # STATIC PROMPT MODE FOR TOOL_EVAL (MV_Resolvers)
            # No dynamic UDF - use static prompt with @Prompt@ replacement
            print(f"    🎯 Static Prompt Batch Submit (MV_Resolvers tool_eval)", flush=True)
            
            # Count pending records
            count_sql = f"""
                SELECT COUNT(*) AS pending_count
                FROM {table_name}
                WHERE PROCESSING_STATUS = 'PENDING'
                  AND DEPARTMENT = '{department_name}'
                  AND DATE = '{target_date_str}'
            """
            pending_count = session.sql(count_sql).collect()[0]['PENDING_COUNT']
            
            if pending_count == 0:
                print(f"    ⚠️  No pending records to submit", flush=True)
                return True, None, None, 0
            
            print(f"    [{datetime.now().strftime('%H:%M:%S')}] 📊 Submitting batch for {pending_count} records...", flush=True)
            
            # Escape system prompt for SQL
            system_prompt_escaped = system_text.replace("'", "''")
            
            # Build batch submission SQL with @Prompt@ replacement
            batch_submit_sql = f"""
                WITH base AS (
                    SELECT
                        MESSAGE_ID,
                        MESSAGE,
                        MODEL_NAME,
                        TEMPERATURE,
                        MAX_TOKENS
                    FROM {table_name}
                    WHERE PROCESSING_STATUS = 'PENDING'
                        AND DEPARTMENT = '{department_name}'
                        AND DATE = '{target_date_str}'
                ),
                prompts AS (
                    SELECT
                        MESSAGE_ID,
                        MESSAGE,
                        REPLACE($${system_prompt_escaped}$$, '@Prompt@', MESSAGE) AS INSTRUCTIONS,
                        MODEL_NAME,
                        TEMPERATURE,
                        MAX_TOKENS
                    FROM base
                ),
                model_params AS (
                    SELECT
                        ANY_VALUE(MODEL_NAME) AS MODEL_NAME,
                        ANY_VALUE(TEMPERATURE) AS TEMPERATURE,
                        ANY_VALUE(MAX_TOKENS) AS MAX_TOKENS
                    FROM prompts
                ),
                items AS (
                    SELECT
                        ARRAY_AGG(
                            OBJECT_CONSTRUCT(
                                'custom_id', MESSAGE_ID,
                                'input', MESSAGE,
                                'instructions', INSTRUCTIONS
                            )
                        ) WITHIN GROUP (ORDER BY MESSAGE_ID) AS arr
                    FROM prompts
                ),
                submit AS (
                    SELECT {llm_function}(
                        NULL,
                        NULL,
                        (SELECT MODEL_NAME FROM model_params),
                        (SELECT TEMPERATURE FROM model_params),
                        (SELECT MAX_TOKENS FROM model_params),
                        '{reasoning_effort}',
                        '{department_name}',
                        'batch_submit',
                        TO_JSON(arr),
                        NULL,
                        NULL,
                        {response_format_param}
                    ) AS resp
                    FROM items
                )
                SELECT
                    resp:batch_id::string AS BATCH_ID,
                    resp:status::string AS BATCH_STATUS,
                    resp:input_file_id::string AS INPUT_FILE_ID
                FROM submit
            """
            
            print(f"    [{datetime.now().strftime('%H:%M:%S')}] ⏳ Executing static prompt batch submit SQL...", flush=True)
            
            # Execute batch submit
            result = session.sql(batch_submit_sql).collect()
            
            if not result:
                print(f"    ❌ Batch submission failed - no result", flush=True)
                return False, None, None, 0
            
            batch_id = result[0]['BATCH_ID']
            batch_status = result[0]['BATCH_STATUS']
            input_file_id = result[0]['INPUT_FILE_ID']
            
            print(f"    ✅ Static prompt batch submitted successfully!", flush=True)
            print(f"    📝 Batch ID: {batch_id}", flush=True)
            print(f"    📁 Input File ID: {input_file_id}", flush=True)
            print(f"    📊 Total records: {pending_count}", flush=True)
            
            return True, batch_id, input_file_id, pending_count
        
        # Count pending records
        if per_skill:
            allowed_skills = [str(s).upper() for s in system_text.keys()]
            skill_list_sql = ", ".join(["'{}'".format(s.replace("'", "''")) for s in allowed_skills])
            count_sql = f"""
                SELECT COUNT(*) AS pending_count
                FROM {table_name}
                WHERE PROCESSING_STATUS = 'PENDING'
                  AND DEPARTMENT = '{department_name}'
                  AND DATE = '{target_date_str}'
                  AND UPPER(LAST_SKILL) IN ({skill_list_sql})
            """
        elif per_user_type_mode:
            # No special filtering needed for user type mode - all pending records
            count_sql = f"""
                SELECT COUNT(*) AS pending_count
                FROM {table_name}
                WHERE PROCESSING_STATUS = 'PENDING'
                  AND DEPARTMENT = '{department_name}'
                  AND DATE = '{target_date_str}'
            """
        else:
            count_sql = f"""
                SELECT COUNT(*) AS pending_count
                FROM {table_name}
                WHERE PROCESSING_STATUS = 'PENDING'
                  AND DEPARTMENT = '{department_name}'
                  AND DATE = '{target_date_str}'
            """
        
        pending_count = session.sql(count_sql).collect()[0]['PENDING_COUNT']
        
        if pending_count == 0:
            print(f"    ⚠️  No pending records to submit", flush=True)
            return True, None, None, 0
        
        print(f"    [{datetime.now().strftime('%H:%M:%S')}] 📊 Submitting batch for {pending_count} records...", flush=True)
        print(f"    [{datetime.now().strftime('%H:%M:%S')}] ⏳ Building batch submission SQL query (this may take a moment for large batches)...", flush=True)
        
        # Wrap CONVERSATION_CONTENT for OpenAI when using JSON format (exclude tool_eval)
        print(f"    🔍 JSON wrapping check: response_format={response_format}, model_type={model_type}, prompt_type={prompt_type}")
        if response_format == 'json_object' and model_type == 'openai' and prompt_type != 'tool_eval':
            print(f"    ✅ Adding JSON prefix to CONVERSATION_CONTENT")
            content_field = "CONCAT('Return your response as JSON.\\n\\n', CONVERSATION_CONTENT)"
        else:
            print(f"    ⏭️  Skipping JSON prefix (response_format={response_format}, model_type={model_type}, prompt_type={prompt_type})")
            content_field = "CONVERSATION_CONTENT"
        
        # Build batch submission SQL
        if per_skill:
            # Per-skill mode: different prompts per LAST_SKILL
            allowed_skills = [str(s).upper() for s in system_text.keys()]
            skill_list_sql = ", ".join(["'{}'".format(s.replace("'", "''")) for s in allowed_skills])
            
            case_branches = []
            for skill, prompt in system_text.items():
                safe_skill = str(skill).upper().replace("'", "''")
                case_branches.append(f"WHEN UPPER(LAST_SKILL) = '{safe_skill}' THEN $${prompt}$$")
            case_expr = "CASE " + " ".join(case_branches) + " END"
            
            batch_submit_sql = f"""
                WITH base AS (
                SELECT
                    CONVERSATION_ID,
                    CONVERSATION_CONTENT,
                    LAST_SKILL,
                    EXECUTION_ID,
                    MODEL_NAME,
                    TEMPERATURE,
                    MAX_TOKENS
                FROM {table_name}
                WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                    AND DATE = '{target_date_str}'
                    AND UPPER(LAST_SKILL) IN ({skill_list_sql})
                ),
                prompts AS (
                SELECT
                    CONVERSATION_ID,
                    CONVERSATION_CONTENT,
                    REPLACE(
                    REPLACE(
                        {case_expr},
                        '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')
                    ),
                    '<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>')
                    ) AS INSTRUCTIONS,
                    MODEL_NAME,
                    TEMPERATURE,
                    MAX_TOKENS
                FROM base
                ),
                model_params AS (
                SELECT
                    ANY_VALUE(MODEL_NAME)    AS MODEL_NAME,
                    ANY_VALUE(TEMPERATURE)   AS TEMPERATURE,
                    ANY_VALUE(MAX_TOKENS)    AS MAX_TOKENS
                FROM prompts
                ),
                items AS (
                SELECT
                    ARRAY_AGG(
                    OBJECT_CONSTRUCT(
                        'custom_id',    CONVERSATION_ID,
                        'input',        {content_field},
                        'instructions', INSTRUCTIONS
                    )
                    ) WITHIN GROUP (ORDER BY CONVERSATION_ID) AS arr
                FROM prompts
                ),
                submit AS (
                SELECT {llm_function}(
                        NULL,
                        NULL,
                        (SELECT MODEL_NAME  FROM model_params),
                        (SELECT TEMPERATURE FROM model_params),
                        (SELECT MAX_TOKENS  FROM model_params),
                        '{reasoning_effort}',
                        '{department_name}',
                        'batch_submit',
                        TO_JSON(arr),
                        NULL,
                        NULL,
                        {response_format_param}
                        ) AS resp
                FROM items
                )
                SELECT
                resp:batch_id::string      AS BATCH_ID,
                resp:status::string        AS BATCH_STATUS,
                resp:input_file_id::string AS INPUT_FILE_ID
                FROM submit
            """
        elif per_user_type_mode:
            # User type mode: different prompts per USER_TYPE
            case_branches = []
            for user_type, prompt in system_text_by_user_type.items():
                safe_user_type = str(user_type).upper().replace("'", "''")
                case_branches.append(f"WHEN UPPER(USER_TYPE) = '{safe_user_type}' THEN $${prompt}$$")
            # Add default case (client prompt)
            default_prompt = system_text_by_user_type.get('client', system_text)
            case_branches.append(f"ELSE $${default_prompt}$$")
            case_expr = "CASE " + " ".join(case_branches) + " END"
            
            batch_submit_sql = f"""
                WITH base AS (
                SELECT
                    CONVERSATION_ID,
                    CONVERSATION_CONTENT,
                    USER_TYPE,
                    LAST_SKILL,
                    EXECUTION_ID,
                    MODEL_NAME,
                    TEMPERATURE,
                    MAX_TOKENS
                FROM {table_name}
                WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                    AND DATE = '{target_date_str}'
                ),
                prompts AS (
                SELECT
                    CONVERSATION_ID,
                    CONVERSATION_CONTENT,
                    REPLACE(
                    REPLACE(
                        {case_expr},
                        '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')
                    ),
                    '<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>')
                    ) AS INSTRUCTIONS,
                    MODEL_NAME,
                    TEMPERATURE,
                    MAX_TOKENS
                FROM base
                ),
                model_params AS (
                SELECT
                    ANY_VALUE(MODEL_NAME)    AS MODEL_NAME,
                    ANY_VALUE(TEMPERATURE)   AS TEMPERATURE,
                    ANY_VALUE(MAX_TOKENS)    AS MAX_TOKENS
                FROM prompts
                ),
                items AS (
                SELECT
                    ARRAY_AGG(
                    OBJECT_CONSTRUCT(
                        'custom_id',    CONVERSATION_ID,
                        'input',        {content_field},
                        'instructions', INSTRUCTIONS
                    )
                    ) WITHIN GROUP (ORDER BY CONVERSATION_ID) AS arr
                FROM prompts
                ),
                submit AS (
                SELECT {llm_function}(
                        NULL,
                        NULL,
                        (SELECT MODEL_NAME  FROM model_params),
                        (SELECT TEMPERATURE FROM model_params),
                        (SELECT MAX_TOKENS  FROM model_params),
                        '{reasoning_effort}',
                        '{department_name}',
                        'batch_submit',
                        TO_JSON(arr),
                        NULL,
                        NULL,
                        {response_format_param}
                        ) AS resp
                FROM items
                )
                SELECT
                resp:batch_id::string      AS BATCH_ID,
                resp:status::string        AS BATCH_STATUS,
                resp:input_file_id::string AS INPUT_FILE_ID,
                resp                       AS RAW_RESPONSE
                FROM submit
            """
        else:
            # Single prompt mode
            batch_submit_sql = f"""
                WITH base AS (
                SELECT
                    CONVERSATION_ID,
                    CONVERSATION_CONTENT,
                    LAST_SKILL,
                    EXECUTION_ID,
                    MODEL_NAME,
                    TEMPERATURE,
                    MAX_TOKENS
                FROM {table_name}
                WHERE PROCESSING_STATUS = 'PENDING'
                    AND DEPARTMENT = '{department_name}'
                    AND DATE = '{target_date_str}'
                ),
                prompts AS (
                SELECT
                    CONVERSATION_ID,
                    CONVERSATION_CONTENT,
                    REPLACE(
                    REPLACE(
                        $${system_text}$$,
                        '@Prompt@', COALESCE(GET_N8N_SYSTEM_PROMPT(EXECUTION_ID, '{department_name}'), GET_ERP_SYSTEM_PROMPT(CONVERSATION_ID), '@Prompt@')
                    ),
                    '<STEP-NAME>', COALESCE(LAST_SKILL, '<STEP-NAME>')
                    ) AS INSTRUCTIONS,
                    MODEL_NAME,
                    TEMPERATURE,
                    MAX_TOKENS
                FROM base
                ),
                model_params AS (
                SELECT
                    ANY_VALUE(MODEL_NAME)    AS MODEL_NAME,
                    ANY_VALUE(TEMPERATURE)   AS TEMPERATURE,
                    ANY_VALUE(MAX_TOKENS)    AS MAX_TOKENS
                FROM prompts
                ),
                items AS (
                SELECT
                    ARRAY_AGG(
                    OBJECT_CONSTRUCT(
                        'custom_id',    CONVERSATION_ID,
                        'input',        {content_field},
                        'instructions', INSTRUCTIONS
                    )
                    ) WITHIN GROUP (ORDER BY CONVERSATION_ID) AS arr
                FROM prompts
                ),
                submit AS (
                SELECT {llm_function}(
                        NULL,
                        NULL,
                        (SELECT MODEL_NAME  FROM model_params),
                        (SELECT TEMPERATURE FROM model_params),
                        (SELECT MAX_TOKENS  FROM model_params),
                        '{reasoning_effort}',
                        '{department_name}',
                        'batch_submit',
                        TO_JSON(arr),
                        NULL,
                        NULL,
                        {response_format_param}
                        ) AS resp
                FROM items
                )
                SELECT
                resp:batch_id::string      AS BATCH_ID,
                resp:status::string        AS BATCH_STATUS,
                resp:input_file_id::string AS INPUT_FILE_ID,
                resp                       AS RAW_RESPONSE
                FROM submit
            """
        
        # Execute submission
        print(f"    [{datetime.now().strftime('%H:%M:%S')}] 🚀 Executing batch submission SQL (uploading to {model_type.upper()} - this may take 2-10 minutes for large batches)...", flush=True)
        submit_row = session.sql(batch_submit_sql).collect()[0]
        batch_id = submit_row['BATCH_ID']
        batch_status = submit_row['BATCH_STATUS']
        input_file_id = submit_row['INPUT_FILE_ID']
        raw_response = submit_row['RAW_RESPONSE']
        
        print(f"    [{datetime.now().strftime('%H:%M:%S')}] ✅ Batch submitted successfully", flush=True)
        print(f"       [{datetime.now().strftime('%H:%M:%S')}] Batch ID: {batch_id}")
        print(f"       Status: {batch_status}")
        print(f"       Records: {pending_count}")
        print(f"       Raw response: {raw_response}")
        
        return True, batch_id, input_file_id, pending_count
    
    except Exception as e:
        error_msg = f"Batch submission failed: {str(e)}"
        print(f"    [{datetime.now().strftime('%H:%M:%S')}] ❌ {error_msg}")
        print(format_error_details(e, f"BATCH SUBMIT - {department_name}"))
        return False, None, None, 0


def run_batch_llm_fetch(session: snowpark.Session, batch_id, table_name, department_name, target_date, model_name=None, prompt_type=None):
    """
    Fetch completed batch results from OpenAI/Gemini Batch API
    
    This function checks the batch status and fetches results if completed or cancelled.
    If not completed, it returns False (timeout/not ready).
    
    Args:
        session: Snowflake session
        batch_id: Batch ID (OpenAI or Gemini)
        table_name: Output table name
        department_name: Department name
        target_date: Target date
        model_name: Model name (e.g., 'gpt-5-mini', 'gemini-2.5-flash') to determine LLM function
        prompt_type: Prompt type (e.g., 'tool_eval', 'SA_prompt', etc.)
    
    Returns:
        Tuple: (success: bool, processed_count: int, failed_count: int, status: str)
    """
    import time
    
    try:
        target_date_str = target_date if target_date else datetime.now().strftime("%Y-%m-%d")
        
        # Determine model_type and llm_function from model_name AND prompt_type
        if model_name:
            if 'gemini' in model_name.lower():
                model_type = 'gemini'
                llm_function = 'gemini_chat_system'
            else:
                model_type = 'openai'
                # For tool_eval, use Chat Completions API
                if prompt_type == 'tool_eval':
                    llm_function = 'openai_chat_system_completions'
                else:
                    llm_function = 'openai_chat_system'
        else:
            # Default to OpenAI for backward compatibility
            model_type = 'openai'
            llm_function = 'openai_chat_system'
        
        print(f"    📥 Fetching {model_type.upper()} batch results for {batch_id}...")
        
        # Step 1: Check batch status
        status_sql = f"""
        WITH r AS (
        SELECT {llm_function}(
                NULL, NULL,
                '{model_name or 'gpt-5-mini'}', 0, 0, 'minimal',
                '{department_name}',
                'batch_status',
                NULL,
                '{batch_id}'
                ) AS v
        )
        SELECT
        v:status::string         AS STATUS,
        v:request_counts         AS REQUEST_COUNTS,
        v:error                  AS ERROR
        FROM r
        """
        
        status_row = session.sql(status_sql).collect()[0]
        status = (status_row['STATUS'] or '').lower()
        request_counts = status_row['REQUEST_COUNTS']
        error = status_row['ERROR']
        
        print(f"    📊 Batch status: {status}")
        if request_counts:
            print(f"       Request counts: {request_counts}")
        
        # Step 2: Check if completed or cancelled (partial results may be available)
        terminal_statuses_with_results = ['completed', 'cancelled']
        
        if status not in terminal_statuses_with_results:
            print(f"    ⏳ Batch not ready yet (status: {status})")
            if error:
                print(f"       Error: {error}")
            return False, 0, 0, status
        
        # Step 3: Fetch results (full or partial)
        if status == 'cancelled':
            print(f"    ⚠️  Batch was cancelled - fetching partial results...")
        else:
            print(f"    ✅ Batch completed! Fetching results...")
        
        fetch_sql = f"""
        MERGE INTO {table_name} AS T
        USING (
        SELECT
            f.value:custom_id::string AS CONVERSATION_ID,
            f.value:text::string      AS LLM_RESPONSE,
            TO_VARCHAR(f.value:usage) AS TOKENS_BREAKDOWN
        FROM (
            SELECT {llm_function}(
                    NULL, NULL,
                    '{model_name or 'gpt-5-mini'}', 0, 0, 'minimal',
                    '{department_name}',
                    'batch_result',
                    NULL,
                    '{batch_id}'
                ) AS v
        ) r,
        LATERAL FLATTEN(input => r.v:results) f
        ) AS S
        ON T.CONVERSATION_ID = S.CONVERSATION_ID
        WHEN MATCHED THEN UPDATE SET
        T.LLM_RESPONSE      = S.LLM_RESPONSE,
        T.TOKENS_BREAKDOWN  = S.TOKENS_BREAKDOWN,
        T.PROCESSING_STATUS = 'COMPLETED',
        T.DATE              = DATE('{target_date_str}');
        """
        
        session.sql(fetch_sql).collect()
        print(f"    ✅ Results merged into {table_name}")
        
        # Step 4: Count results
        processed_count, failed_count = count_llm_results(session, table_name, department_name, target_date)
        
        print(f"    📊 Results: {processed_count} processed, {failed_count} failed")
        
        # Step 5: Update BATCH_TRACKING table to mark as completed
        # Note: We don't have prompt_type here, so this would need to be passed from the caller
        # For now, we'll skip this and update in the phase2 function instead
        
        return True, processed_count, failed_count, status
    
    except Exception as e:
        error_msg = f"Batch fetch failed: {str(e)}"
        print(f"    ❌ {error_msg}")
        print(format_error_details(e, f"BATCH FETCH - {batch_id}"))
        return False, 0, 0, 'error'


def update_batch_tracking(session: snowpark.Session, date, department, prompt_type, batch_info):
    """
    Insert or update batch tracking record using existing insert_raw_data_with_cleanup
    
    This function will ONLY delete the previous record for the specific PROMPT_TYPE
    before inserting the new record, preventing deletion of other prompts' batch records.
    
    Args:
        session: Snowflake session
        date: Target date
        department: Department name
        prompt_type: Prompt type
        batch_info: Dictionary with batch information:
            {
                'PROMPT_TYPE': 'SA_prompt',
                'BATCH_ID': 'batch_xyz',
                'BATCH_STATUS': 'submitted',
                'INPUT_FILE_ID': 'file_xyz',
                'OUTPUT_TABLE': 'SA_RAW_DATA',
                'MODEL_NAME': 'gpt-5',
                'CONVERSION_TYPE': 'xml',
                'TOTAL_REQUESTS': 1234,
                'ERROR_MESSAGE': ''
            }
    
    Returns:
        bool: Success status
    """
    try:
        # Create DataFrame with single row
        df = pd.DataFrame([batch_info])
        
        # Use existing function to insert with additional filter for PROMPT_TYPE
        # This ensures only the record for THIS prompt is deleted, not all prompts
        result = insert_raw_data_with_cleanup(
            session=session,
            table_name='BATCH_TRACKING',
            department=department,
            target_date=date,
            dataframe=df,
            columns=list(batch_info.keys()),
            additional_filter={'PROMPT_TYPE': prompt_type}  # Only delete records for this specific prompt
        )
        
        return result.get('status') == 'success'
    
    except Exception as e:
        print(f"    ❌ Failed to update batch tracking: {str(e)}")
        return False


def fetch_batch_tracking_records(session: snowpark.Session, date, department=None):
    """
    Query BATCH_TRACKING table to get batch information
    
    Args:
        session: Snowflake session
        date: Target date
        department: Optional department filter
    
    Returns:
        List of dictionaries with batch information
    """
    try:
        where_clause = f"WHERE DATE = '{date}'"
        if department:
            where_clause += f" AND DEPARTMENT = '{department}'"
        
        query = f"""
        SELECT 
            PROMPT_TYPE,
            BATCH_ID,
            BATCH_STATUS,
            INPUT_FILE_ID,
            OUTPUT_TABLE,
            MODEL_NAME,
            CONVERSION_TYPE,
            TOTAL_REQUESTS,
            ERROR_MESSAGE
        FROM LLM_EVAL.PUBLIC.BATCH_TRACKING
        {where_clause}
        ORDER BY PROMPT_TYPE
        """
        
        results = session.sql(query).collect()
        return [row.as_dict() for row in results]
    
    except Exception as e:
        print(f"    ⚠️  Failed to fetch batch tracking records: {str(e)}")
        return []


def count_llm_results(session: snowpark.Session, table_name, department_name, target_date):
    """
    Count successful and failed LLM responses by checking for error patterns
    
    Args:
        session: Snowflake session
        table_name: Full table name
        department_name: Department name
        target_date: Target date
    
    Returns:
        Tuple: (processed_count, failed_count)
    """
    import time
    
    try:
        # Count total completed records with timing
        total_start_time = time.time()
        
        total_query = f"""
        SELECT COUNT(*) as total_count
        FROM {table_name}
        WHERE PROCESSING_STATUS = 'COMPLETED'
        AND DEPARTMENT = '{department_name}'
        AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
        """
        
        total_result = session.sql(total_query).collect()
        total_count = total_result[0]['TOTAL_COUNT'] if total_result else 0
        
        total_time = time.time() - total_start_time
        
        # Count failed records (those with error patterns) with timing
        failed_start_time = time.time()
        
        failed_query = f"""
        SELECT COUNT(*) as failed_count
        FROM {table_name}
        WHERE PROCESSING_STATUS = 'COMPLETED'
        AND DEPARTMENT = '{department_name}'
        AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
        AND (
            LLM_RESPONSE LIKE '%[gemini_chat error]%'
            OR LLM_RESPONSE LIKE '%[openai_chat error]%'
            OR LLM_RESPONSE IS NULL
            OR LLM_RESPONSE = ''
        )
        """
        
        failed_result = session.sql(failed_query).collect()
        failed_count = failed_result[0]['FAILED_COUNT'] if failed_result else 0
        
        failed_time = time.time() - failed_start_time
        
        processed_count = total_count - failed_count
        success_rate = (processed_count / total_count * 100) if total_count > 0 else 0
        
        print(f"    📊 Results: {processed_count} successful, {failed_count} failed out of {total_count} total ({success_rate:.1f}% success)")
        print(f"    ⏱️  Counting times: Total query {total_time:.2f}s, Failed query {failed_time:.2f}s")
        
        # Quality insights
        if success_rate < 90 and total_count > 0:
            print(f"    ⚠️  LOW SUCCESS RATE: {success_rate:.1f}% - Check LLM function issues")
        
        if total_time > 2 or failed_time > 2:
            print(f"    🐌 SLOW COUNTING: Consider adding indexes on PROCESSING_STATUS, DEPARTMENT, DATE")
        
        return processed_count, failed_count
        
    except Exception as e:
        print(f"    ⚠️  Error counting results: {str(e)}")
        return 0, 0


def count_llm_results_with_extra_filter(session: snowpark.Session, table_name, department_name, target_date, extra_where_clause):
    """
    Variant of count_llm_results that accepts an additional WHERE clause snippet
    to narrow counting (e.g., by LAST_SKILL for per-skill prompts).
    """
    import time
    try:
        total_start_time = time.time()
        total_query = f"""
        SELECT COUNT(*) as total_count
        FROM {table_name}
        WHERE PROCESSING_STATUS = 'COMPLETED'
        AND DEPARTMENT = '{department_name}'
        AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
        {extra_where_clause}
        """
        total_result = session.sql(total_query).collect()
        total_count = total_result[0]['TOTAL_COUNT'] if total_result else 0

        failed_start_time = time.time()
        failed_query = f"""
        SELECT COUNT(*) as failed_count
        FROM {table_name}
        WHERE PROCESSING_STATUS = 'COMPLETED'
        AND DEPARTMENT = '{department_name}'
        AND DATE = '{target_date if target_date else datetime.now().strftime("%Y-%m-%d")}'
        {extra_where_clause}
        AND (
            LLM_RESPONSE LIKE '%[gemini_chat error]%'
            OR LLM_RESPONSE LIKE '%[openai_chat error]%'
            OR LLM_RESPONSE IS NULL
            OR LLM_RESPONSE = ''
        )
        """
        failed_result = session.sql(failed_query).collect()
        failed_count = failed_result[0]['FAILED_COUNT'] if failed_result else 0

        processed_count = total_count - failed_count
        return processed_count, failed_count
    except Exception as e:
        print(f"    ⚠️  Error counting results (filtered): {str(e)}")
        return 0, 0


def process_department_llm_analysis(session: snowpark.Session, department_name, target_date=None, selected_prompts=None, execution_mode='synchronous', max_conversations: int = None):
    """
    Process LLM analysis for a single department - supports four execution modes
    
    Args:
        session: Snowflake session
        department_name: Department name to process
        target_date: Target date for analysis
        selected_prompts: List of prompt types to process (default: all prompts)
        execution_mode: 'synchronous' | 'batch_submit' | 'batch_fetch' | 'batch_cancel' (default: 'synchronous')
        max_conversations: Optional int. If set in synchronous mode, limit prompt processing to at most this many
                          unique CONVERSATION_IDs (useful for testing cost control).
    
    Returns:
        Tuple: (department_results, success)
    """
    # Route to appropriate function based on execution mode
    if execution_mode == 'batch_submit':
        return process_department_llm_analysis_batch_phase1(
            session, department_name, target_date, selected_prompts
        )
    elif execution_mode == 'batch_fetch':
        return process_department_llm_analysis_batch_phase2(
            session, department_name, target_date, selected_prompts
        )
    elif execution_mode == 'batch_cancel':
        return process_department_llm_analysis_batch_cancel(
            session, department_name, target_date, selected_prompts
        )
    
    # SYNCHRONOUS MODE (default - existing implementation)
    print(f"\n🤖 PROCESSING LLM ANALYSIS: {department_name}")
    print("=" * 50)
    
    try:
        # Step 1: Get filtered data using existing Phase 1 foundation
        print(f"📊 Step 1: Loading filtered data...")
        filtered_df, phase1_stats, success = process_department_phase1(
            session, department_name, target_date
        )
        
        if not success or filtered_df.empty:
            print(f"    ❌ No filtered data from Phase 1")
            return {'error': 'No filtered data from Phase 1'}, False
        
        print(f"    ✅ Loaded {len(filtered_df)} rows, {filtered_df['CONVERSATION_ID'].nunique()} conversations")
        
        # Step 2: Get department configuration for prompt processing
        departments_config = get_snowflake_llm_departments_config()
        dept_config = departments_config[department_name]
        
        if 'llm_prompts' not in dept_config or not dept_config['llm_prompts']:
            print(f"    ⚠️  No LLM prompts configured for {department_name} - skipping prompt processing")
            # Return success with empty results instead of error
            return {}, True
        
        # Step 3: Process each prompt type with appropriate conversion
        department_results = {}
        
        # Filter prompts if a subset was requested
        all_prompts = dept_config['llm_prompts']
        if selected_prompts is None or selected_prompts == ['*']:
            prompts_to_run = all_prompts
        else:
            selected_set = set(selected_prompts)
            prompts_to_run = {k: v for k, v in all_prompts.items() if (k in selected_set)}
            missing = [p for p in selected_prompts if p not in all_prompts]
            if missing:
                print(f"    ⚠️  Skipping unknown prompts for {department_name}: {missing}")

        for prompt_type, prompt_config in prompts_to_run.items():
            print(f"  🎯 Processing prompt: {prompt_type}")
            
            # Step 2a: Choose conversion method based on prompt config
            conversion_type = prompt_config.get('conversion_type', 'xml')  # Default to XML
            
            if prompt_type == 'misprescription':
                print(f"    🔄 Filtering for misprescription...")
                filtered_df_2 = filter_conversations_by_category(session, filtered_df, 'OTC Medication Advice', department_name, target_date)
            elif prompt_type == 'unnecessary_clinic' or prompt_type == 'clinic_recommendation_reason':
                print(f"    🔄 Filtering for unnecessary clinic...")
                filtered_df_2 = filter_conversations_by_category(session, filtered_df, 'Clinic Recommendation', department_name, target_date)
            elif prompt_type == 'agent_intervention':
                print(f"    🔄 Filtering for agent intervention...")
                # Get conversation IDs that have at least one agent message
                conversations_with_agent = filtered_df[
                    filtered_df['SENT_BY'].str.lower() == 'agent'
                ]['CONVERSATION_ID'].unique()
                # Filter to include all rows from conversations that have at least one agent message
                filtered_df_2 = filtered_df[
                    filtered_df['CONVERSATION_ID'].isin(conversations_with_agent)
                ]
                print(f"    ✅ Filtered to {filtered_df_2['CONVERSATION_ID'].nunique()} conversations with agent intervention")
            else:
                filtered_df_2 = filtered_df

            # Optional: limit to a small number of conversations per prompt (token/cost control for testing)
            # This is applied AFTER any per-prompt filtering so rare prompts (e.g., misprescription) still get data.
            if isinstance(max_conversations, int) and max_conversations > 0:
                try:
                    unique_ids = filtered_df_2['CONVERSATION_ID'].dropna().unique().tolist()
                    if len(unique_ids) > max_conversations:
                        before_rows = len(filtered_df_2)
                        before_convs = len(unique_ids)
                        keep_ids = set(sorted(unique_ids, key=lambda x: str(x))[:max_conversations])
                        filtered_df_2 = filtered_df_2[filtered_df_2['CONVERSATION_ID'].isin(keep_ids)].copy()
                        print(f"    🧪 TEST SAMPLE ({prompt_type}): limited to {len(keep_ids)}/{before_convs} conversations ({len(filtered_df_2)}/{before_rows} rows)")
                    else:
                        print(f"    🧪 TEST SAMPLE ({prompt_type}): {len(unique_ids)} conversations <= max_conversations={max_conversations}; no sampling applied")
                except Exception as _e:
                    print(f"    ⚠️  TEST SAMPLE ({prompt_type}) requested but failed to apply: {str(_e)}")

            # Optional token/correctness safeguard: exclude agent messages and stop at first agent intervention (prompt-config driven)
            if bool(prompt_config.get('exclude_agent_and_after_agent', False)):
                try:
                    before_rows = len(filtered_df_2)
                    before_convs = filtered_df_2['CONVERSATION_ID'].nunique() if 'CONVERSATION_ID' in filtered_df_2.columns else 0
                    filtered_df_2 = _exclude_agent_and_after_agent_intervention(filtered_df_2)
                    after_rows = len(filtered_df_2)
                    after_convs = filtered_df_2['CONVERSATION_ID'].nunique() if 'CONVERSATION_ID' in filtered_df_2.columns else 0
                    print(f"    🔒 Agent exclusion ({prompt_type}): {after_convs}/{before_convs} conversations, {after_rows}/{before_rows} rows kept")
                except Exception as _e:
                    print(f"    ⚠️  Agent exclusion ({prompt_type}) requested but failed to apply: {str(_e)}")
            
            if conversion_type == 'xml':
                print(f"    🔄 Converting to XML format for {prompt_type}...")
                from snowflake_llm_xml_converter import convert_conversations_to_xml_dataframe, validate_xml_conversion
                
                if department_name == 'MV_Resolvers':
                    include_time_stamps = True
                else:
                    include_time_stamps = False

                conversations_df = convert_conversations_to_xml_dataframe(filtered_df_2, department_name, include_time_stamps=include_time_stamps)
                if conversations_df.empty:
                    print(f"    ❌ No conversations converted to XML for {prompt_type}")
                    department_results[prompt_type] = {'error': 'No XML conversations', 'conversion_type': 'xml'}
                    continue
                
                # Validate conversion
                validation_results = validate_xml_conversion(conversations_df, department_name)
                print(f"    ✅ XML conversion: {validation_results['valid_xml_count']}/{validation_results['total_conversations']} valid ({validation_results['success_rate']:.1f}%)")
                
                # Rename column for consistency
                conversations_df['conversation_content'] = conversations_df['content_xml_view']
                
            elif conversion_type == 'segment':
                print(f"    🔄 Converting to segment format for {prompt_type}...")
                from snowflake_llm_segment_converter import convert_conversations_to_segment_dataframe, validate_segment_conversion
                
                conversations_df = convert_conversations_to_segment_dataframe(filtered_df_2, department_name)
                if conversations_df.empty:
                    print(f"    ❌ No conversations converted to segment for {prompt_type}")
                    department_results[prompt_type] = {'error': 'No segment conversations', 'conversion_type': 'segment'}
                    continue
                
                # Validate conversion
                validation_results = validate_segment_conversion(conversations_df, department_name)
                print(f"    ✅ Segment conversion: {validation_results['valid_segment_count']}/{validation_results['total_bot_segments']} valid BOT segments ({validation_results['success_rate']:.1f}%) from {validation_results['unique_conversations']} conversations")
                
                # Rename column for consistency
                conversations_df['conversation_content'] = conversations_df['messages']
                
            elif conversion_type == 'json':
                print(f"    🔄 Converting to JSON format for {prompt_type}...")
                from snowflake_llm_json_converter import convert_conversations_to_json_dataframe, validate_json_conversion

                if prompt_type == 'tool' and 'at_filipina' in department_name.lower():
                    include_tool_output = True
                elif prompt_type == 'negative_tool_response':
                    include_tool_output = True
                else:
                    include_tool_output = False

                conversations_df = convert_conversations_to_json_dataframe(filtered_df_2, department_name, include_tool_output=include_tool_output)
                if conversations_df.empty:
                    print(f"    ❌ No conversations converted to JSON for {prompt_type}")
                    department_results[prompt_type] = {'error': 'No JSON conversations', 'conversion_type': 'json'}
                    continue
                
                # Validate conversion
                validation_results = validate_json_conversion(conversations_df, department_name)
                print(f"    ✅ JSON conversion: {validation_results['valid_json_count']}/{validation_results['total_conversations']} valid JSON conversations ({validation_results['success_rate']:.1f}%)")
                
                # Rename column for consistency
                conversations_df['conversation_content'] = conversations_df['content_json_view']
                
                # If this is the loss_interest prompt with per-skill system prompts, pre-filter rows
                if prompt_type == 'loss_interest' and isinstance(prompt_config.get('system_prompt'), dict):
                    allowed_skills = list(prompt_config['system_prompt'].keys())
                    before_len = len(conversations_df)
                    conversations_df = conversations_df[conversations_df['last_skill'].isin(allowed_skills)].copy()
                    print(f"    🎛️ loss_interest: filtered JSON conversations {before_len}→{len(conversations_df)} by LAST_SKILL")
                
            elif conversion_type == 'xml3d':
                print(f"    🔄 Converting to XML3D format for {prompt_type}...")
                from snowflake_llm_xml3d import convert_conversations_to_xml3d, validate_xml3d_conversion

                filtered_df_3d, phase1_stats_3d, success = process_department_phase1_multi_day(
                    session, department_name, target_date
                )
                
                conversations_df = convert_conversations_to_xml3d(filtered_df_3d, department_name)
                if conversations_df.empty:
                    print(f"    ❌ No conversations converted to XML3D for {prompt_type}")
                    department_results[prompt_type] = {'error': 'No XML3D conversations', 'conversion_type': 'xml3d'}
                    continue
                
                # Validate conversion
                validation_results = validate_xml3d_conversion(conversations_df, department_name)
                print(f"    ✅ XML3D conversion: {validation_results['valid_xml3d_count']}/{validation_results['total_conversations']} valid XML3D conversations ({validation_results['success_rate']:.1f}%)")
                
                # Rename column for consistency
                conversations_df['conversation_content'] = conversations_df['content_xml_view']
                
            elif conversion_type == 'xmlcw':
                print(f"    🔄 Converting to XMLCW format for {prompt_type}...")
                from snowflake_llm_xml3d import convert_conversations_to_xmlcw, validate_xml3d_conversion
                
                filtered_df_cw, phase1_stats_cw, success = process_department_phase1_cw(
                    session, department_name, target_date
                )
                
                # Check if CW query returned no data
                if filtered_df_cw.empty or not success:
                    print(f"    ⚠️ No conversations after CW filtering for {prompt_type}")
                    department_results[prompt_type] = {'error': 'No data after CW filtering', 'conversion_type': 'xmlcw'}
                    continue

                conversations_df = convert_conversations_to_xmlcw(filtered_df_cw, department_name)
                if conversations_df.empty:
                    print(f"    ❌ No conversations converted to XMLCW for {prompt_type}")
                    department_results[prompt_type] = {'error': 'No XMLCW conversations', 'conversion_type': 'xmlcw'}
                    continue
                
                # Validate conversion
                validation_results = validate_xml3d_conversion(conversations_df, department_name)
                print(f"    ✅ XMLCW conversion: {validation_results['valid_xml3d_count']}/{validation_results['total_conversations']} valid XMLCW conversations ({validation_results['success_rate']:.1f}%)")
                
                # Rename column for consistency
                conversations_df['conversation_content'] = conversations_df['content_xml_view']
            
            elif conversion_type == 'json_private':
                print(f"    🔄 Converting to JSON private format for {prompt_type}...")
                from snowflake_llm_json_converter import convert_conversations_to_json_dataframe, validate_json_conversion

                conversations_df = convert_conversations_to_json_dataframe(filtered_df_2, department_name, include_private_messages=True)
                if conversations_df.empty:
                    print(f"    ❌ No conversations converted to JSON private for {prompt_type}")
                    department_results[prompt_type] = {'error': 'No JSON private conversations', 'conversion_type': 'json_private'}
                    continue
                
                # Validate conversion
                validation_results = validate_json_conversion(conversations_df, department_name)
                print(f"    ✅ JSON private conversion: {validation_results['valid_json_count']}/{validation_results['total_conversations']} valid JSON private conversations ({validation_results['success_rate']:.1f}%)")
                
                # Rename column for consistency
                conversations_df['conversation_content'] = conversations_df['content_json_view']
            
            elif conversion_type == 'message_segment':
                print(f"    🔄 Converting to message segment format for {prompt_type}...")
                
                # Route converter based on department
                if department_name == 'MV_Resolvers':
                    from snowflake_llm_message_converter import convert_conversations_to_message_segments_mv_resolvers
                    conversations_df = convert_conversations_to_message_segments_mv_resolvers(filtered_df_2, department_name)
                else:
                    # MV_Sales and other departments
                    from snowflake_llm_message_converter import convert_conversations_to_message_segments
                    conversations_df = convert_conversations_to_message_segments(filtered_df_2, department_name)
                
                if conversations_df.empty:
                    print(f"    ❌ No conversations converted to message segment for {prompt_type}")
                    department_results[prompt_type] = {'error': 'No message segment conversations', 'conversion_type': 'message_segment'}
                    continue
                
                # Validate conversion
                # validation_results = validate_message_segment_conversion(conversations_df, department_name)
                # print(f"    ✅ Message segment conversion: {validation_results['valid_message_segment_count']}/{validation_results['total_conversations']} valid message segment conversations ({validation_results['success_rate']:.1f}%)")
                
            else:
                print(f"    ❌ Unknown conversion type: {conversion_type}")
                department_results[prompt_type] = {'error': f'Unknown conversion type: {conversion_type}'}
                continue
            
            # ROUTING LOGIC: Check if this is tool_eval (message-level analysis)
            if prompt_type == 'tool_eval':
                # Use message-level analysis with N8N UDF integration
                print(f"    🎯 Using message-level analysis for tool_eval")
                prompt_results = analyze_messages_with_tool_eval_prompt(
                    session, conversations_df, department_name, prompt_type,
                    prompt_config, target_date, execution_mode='synchronous'
                )
            else:
                # Use conversation-level analysis (existing flow)
                prompt_results = analyze_conversations_with_prompt(
                    session, conversations_df, department_name, prompt_type, 
                    prompt_config, target_date, execution_mode='synchronous'
                )
            
            
            department_results[prompt_type] = prompt_results
        
        # Calculate overall department statistics
        total_prompts = len(department_results)
        successful_prompts = sum(1 for result in department_results.values() 
                               if result.get('processed_count', 0) > 0)
        total_conversations = sum(result.get('total_conversations', 0) for result in department_results.values() 
                                if isinstance(result, dict))
        
        print(f"\n✅ {department_name} COMPLETED:")
        print(f"   🎯 Prompts processed: {successful_prompts}/{total_prompts}")
        print(f"   💬 Conversations analyzed: {total_conversations}")
        
        return department_results, True
        
    except Exception as e:
        error_msg = f"LLM analysis failed: {str(e)}"
        error_details = format_error_details(e, f"DEPARTMENT LLM ANALYSIS - {department_name}")
        print(f"  ❌ {department_name}: {error_msg}")
        print(error_details)
        return {'error': error_msg, 'traceback': str(e)}, False


def process_department_llm_analysis_batch_phase1(session: snowpark.Session, department_name, target_date=None, selected_prompts=None):
    """
    PHASE 1: Submit batches to OpenAI and store batch IDs
    
    This function:
    - Processes Phase 1 foundation to get filtered data
    - Converts data for each prompt
    - Inserts empty records into raw data tables
    - Submits batches to OpenAI
    - Stores batch IDs in BATCH_TRACKING table
    - Returns batch submission results (does NOT wait for completion)
    
    Args:
        session: Snowflake session
        department_name: Department name
        target_date: Target date
        selected_prompts: List of prompt types to process (default: all)
    
    Returns:
        Tuple: (batch_results, success)
    """
    print(f"\n📤 BATCH PHASE 1: SUBMIT BATCHES - {department_name}")
    print("=" * 60)
    
    try:
        # Step 1: Get filtered data using existing Phase 1 foundation
        print(f"📊 Step 1: Loading filtered data...")
        filtered_df, phase1_stats, success = process_department_phase1(
            session, department_name, target_date
        )
        
        if not success or filtered_df.empty:
            print(f"    ❌ No filtered data from Phase 1")
            return {'error': 'No filtered data from Phase 1'}, False
        
        print(f"    ✅ Loaded {len(filtered_df)} rows, {filtered_df['CONVERSATION_ID'].nunique()} conversations")
        
        # Step 2: Get department configuration
        departments_config = get_snowflake_llm_departments_config()
        dept_config = departments_config[department_name]
        
        if 'llm_prompts' not in dept_config or not dept_config['llm_prompts']:
            print(f"    ⚠️  No LLM prompts configured for {department_name}")
            return {}, True
        
        # Step 3: Process each prompt in batch_submit mode
        batch_results = {}
        
        # Filter prompts if a subset was requested
        all_prompts = dept_config['llm_prompts']
        if selected_prompts is None or selected_prompts == ['*']:
            prompts_to_run = all_prompts
        else:
            selected_set = set(selected_prompts)
            prompts_to_run = {k: v for k, v in all_prompts.items() if k in selected_set}
            missing = [p for p in selected_prompts if p not in all_prompts]
            if missing:
                print(f"    ⚠️  Skipping unknown prompts for {department_name}: {missing}")
        
        print(f"\n📝 Processing {len(prompts_to_run)} prompts in BATCH_SUBMIT mode...")
        
        for prompt_type, prompt_config in prompts_to_run.items():
            print(f"\n📤 [{prompt_type}] Preparing batch submission...")
            
            try:
                conversion_type = prompt_config.get('conversion_type', 'xml')
                
                # Apply prompt-specific filtering BEFORE conversion
                if prompt_type == 'misprescription':
                    print(f"    🔄 Filtering for misprescription...")
                    filtered_df_2 = filter_conversations_by_category(session, filtered_df, 'OTC Medication Advice', department_name, target_date)
                elif prompt_type == 'unnecessary_clinic' or prompt_type == 'clinic_recommendation_reason':
                    print(f"    🔄 Filtering for unnecessary clinic...")
                    filtered_df_2 = filter_conversations_by_category(session, filtered_df, 'Clinic Recommendation', department_name, target_date)
                elif prompt_type == 'agent_intervention':
                    print(f"    🔄 Filtering for agent intervention...")
                    conversations_with_agent = filtered_df[
                        filtered_df['SENT_BY'].str.lower() == 'agent'
                    ]['CONVERSATION_ID'].unique()
                    filtered_df_2 = filtered_df[
                        filtered_df['CONVERSATION_ID'].isin(conversations_with_agent)
                    ]
                    print(f"    ✅ Filtered to {filtered_df_2['CONVERSATION_ID'].nunique()} conversations with agent intervention")
                else:
                    filtered_df_2 = filtered_df

                # Optional token/correctness safeguard: exclude agent messages and stop at first agent intervention
                if bool(prompt_config.get('exclude_agent_and_after_agent', False)):
                    filtered_df_2 = _exclude_agent_and_after_agent_intervention(filtered_df_2)
                
                # Convert conversations based on the prompt's conversion type (EXACT same logic as synchronous)
                if conversion_type == 'xml':
                    print(f"    🔄 Converting to XML format for {prompt_type}...")
                    from snowflake_llm_xml_converter import convert_conversations_to_xml_dataframe, validate_xml_conversion
                    
                    if department_name == 'MV_Resolvers':
                        include_time_stamps = True
                    else:
                        include_time_stamps = False

                    conversations_df = convert_conversations_to_xml_dataframe(filtered_df_2, department_name, include_time_stamps=include_time_stamps)
                    if conversations_df.empty:
                        print(f"    ❌ No conversations converted to XML for {prompt_type}")
                        batch_results[prompt_type] = {'error': 'No XML conversations', 'conversion_type': 'xml', 'status': 'failed'}
                        continue
                    
                    validation_results = validate_xml_conversion(conversations_df, department_name)
                    print(f"    ✅ XML conversion: {validation_results['valid_xml_count']}/{validation_results['total_conversations']} valid ({validation_results['success_rate']:.1f}%)")
                    conversations_df['conversation_content'] = conversations_df['content_xml_view']
                    
                elif conversion_type == 'segment':
                    print(f"    🔄 Converting to segment format for {prompt_type}...")
                    from snowflake_llm_segment_converter import convert_conversations_to_segment_dataframe, validate_segment_conversion
                    
                    conversations_df = convert_conversations_to_segment_dataframe(filtered_df_2, department_name)
                    if conversations_df.empty:
                        print(f"    ❌ No conversations converted to segment for {prompt_type}")
                        batch_results[prompt_type] = {'error': 'No segment conversations', 'conversion_type': 'segment', 'status': 'failed'}
                        continue
                    
                    validation_results = validate_segment_conversion(conversations_df, department_name)
                    print(f"    ✅ Segment conversion: {validation_results['valid_segment_count']}/{validation_results['total_bot_segments']} valid BOT segments ({validation_results['success_rate']:.1f}%) from {validation_results['unique_conversations']} conversations")
                    conversations_df['conversation_content'] = conversations_df['messages']
                    
                elif conversion_type == 'json':
                    print(f"    🔄 Converting to JSON format for {prompt_type}...")
                    from snowflake_llm_json_converter import convert_conversations_to_json_dataframe, validate_json_conversion

                    if prompt_type == 'tool' and 'at_filipina' in department_name.lower():
                        include_tool_output = True
                    elif prompt_type == 'negative_tool_response':
                        include_tool_output = True
                    else:
                        include_tool_output = False

                    conversations_df = convert_conversations_to_json_dataframe(filtered_df_2, department_name, include_tool_output=include_tool_output)
                    if conversations_df.empty:
                        print(f"    ❌ No conversations converted to JSON for {prompt_type}")
                        batch_results[prompt_type] = {'error': 'No JSON conversations', 'conversion_type': 'json', 'status': 'failed'}
                        continue
                    
                    validation_results = validate_json_conversion(conversations_df, department_name)
                    print(f"    ✅ JSON conversion: {validation_results['valid_json_count']}/{validation_results['total_conversations']} valid JSON conversations ({validation_results['success_rate']:.1f}%)")
                    conversations_df['conversation_content'] = conversations_df['content_json_view']
                    
                    if prompt_type == 'loss_interest' and isinstance(prompt_config.get('system_prompt'), dict):
                        allowed_skills = list(prompt_config['system_prompt'].keys())
                        before_len = len(conversations_df)
                        conversations_df = conversations_df[conversations_df['last_skill'].isin(allowed_skills)].copy()
                        print(f"    🎛️ loss_interest: filtered JSON conversations {before_len}→{len(conversations_df)} by LAST_SKILL")
                    
                elif conversion_type == 'xml3d':
                    print(f"    🔄 Converting to XML3D format for {prompt_type}...")
                    from snowflake_llm_xml3d import convert_conversations_to_xml3d, validate_xml3d_conversion

                    filtered_df_3d, phase1_stats_3d, success = process_department_phase1_multi_day(
                        session, department_name, target_date
                    )
                    
                    conversations_df = convert_conversations_to_xml3d(filtered_df_3d, department_name)
                    if conversations_df.empty:
                        print(f"    ❌ No conversations converted to XML3D for {prompt_type}")
                        batch_results[prompt_type] = {'error': 'No XML3D conversations', 'conversion_type': 'xml3d', 'status': 'failed'}
                        continue
                    
                    validation_results = validate_xml3d_conversion(conversations_df, department_name)
                    print(f"    ✅ XML3D conversion: {validation_results['valid_xml3d_count']}/{validation_results['total_conversations']} valid XML3D conversations ({validation_results['success_rate']:.1f}%)")
                    conversations_df['conversation_content'] = conversations_df['content_xml_view']
                    
                elif conversion_type == 'xmlcw':
                    print(f"    🔄 Converting to XMLCW format for {prompt_type}...")
                    from snowflake_llm_xml3d import convert_conversations_to_xmlcw, validate_xml3d_conversion
                    
                    filtered_df_cw, phase1_stats_cw, success = process_department_phase1_cw(
                        session, department_name, target_date
                    )
                    
                    # Check if CW query returned no data
                    if filtered_df_cw.empty or not success:
                        print(f"    ⚠️ No conversations after CW filtering for {prompt_type}")
                        batch_results[prompt_type] = {'error': 'No data after CW filtering', 'conversion_type': 'xmlcw', 'status': 'failed'}
                        continue

                    conversations_df = convert_conversations_to_xmlcw(filtered_df_cw, department_name)
                    if conversations_df.empty:
                        print(f"    ❌ No conversations converted to XMLCW for {prompt_type}")
                        batch_results[prompt_type] = {'error': 'No XMLCW conversations', 'conversion_type': 'xmlcw', 'status': 'failed'}
                        continue
                    
                    validation_results = validate_xml3d_conversion(conversations_df, department_name)
                    print(f"    ✅ XMLCW conversion: {validation_results['valid_xml3d_count']}/{validation_results['total_conversations']} valid XMLCW conversations ({validation_results['success_rate']:.1f}%)")
                    conversations_df['conversation_content'] = conversations_df['content_xml_view']
                
                elif conversion_type == 'json_private':
                    print(f"    🔄 Converting to JSON private format for {prompt_type}...")
                    from snowflake_llm_json_converter import convert_conversations_to_json_dataframe, validate_json_conversion

                    conversations_df = convert_conversations_to_json_dataframe(filtered_df_2, department_name, include_private_messages=True)
                    if conversations_df.empty:
                        print(f"    ❌ No conversations converted to JSON private for {prompt_type}")
                        batch_results[prompt_type] = {'error': 'No JSON private conversations', 'conversion_type': 'json_private', 'status': 'failed'}
                        continue
                    
                    validation_results = validate_json_conversion(conversations_df, department_name)
                    print(f"    ✅ JSON private conversion: {validation_results['valid_json_count']}/{validation_results['total_conversations']} valid JSON private conversations ({validation_results['success_rate']:.1f}%)")
                    conversations_df['conversation_content'] = conversations_df['content_json_view']
                elif conversion_type == 'message_segment':
                    print(f"    🔄 Converting to message segment format for {prompt_type}...")
                    
                    # Route converter based on department
                    if department_name == 'MV_Resolvers':
                        from snowflake_llm_message_converter import convert_conversations_to_message_segments_mv_resolvers
                        conversations_df = convert_conversations_to_message_segments_mv_resolvers(filtered_df_2, department_name)
                    else:
                        # MV_Sales and other departments
                        from snowflake_llm_message_converter import convert_conversations_to_message_segments
                        conversations_df = convert_conversations_to_message_segments(filtered_df_2, department_name)
                    
                    if conversations_df.empty:
                        print(f"    ❌ No conversations converted to message segment for {prompt_type}")
                        batch_results[prompt_type] = {'error': 'No message segment conversations', 'conversion_type': 'message_segment'}
                        continue
                    
                    # Validate conversion
                    # validation_results = validate_message_segment_conversion(conversations_df, department_name)
                    # print(f"    ✅ Message segment conversion: {validation_results['valid_message_segment_count']}/{validation_results['total_conversations']} valid message segment conversations ({validation_results['success_rate']:.1f}%)")
                
                
                else:
                    print(f"    ❌ Unknown conversion type: {conversion_type}")
                    batch_results[prompt_type] = {'error': f'Unknown conversion type: {conversion_type}', 'status': 'failed'}
                    continue
                
                # ROUTING LOGIC: Check if this is tool_eval (message-level analysis)
                if prompt_type == 'tool_eval':
                    # Use message-level analysis with N8N UDF integration
                    print(f"    🎯 Using message-level analysis for tool_eval (batch mode)")
                    prompt_results = analyze_messages_with_tool_eval_prompt(
                        session, conversations_df, department_name, prompt_type,
                        prompt_config, target_date, execution_mode='batch_submit'
                    )
                else:
                    # Use conversation-level analysis (existing flow)
                    prompt_results = analyze_conversations_with_prompt(
                        session, conversations_df, department_name, prompt_type, 
                        prompt_config, target_date, execution_mode='batch_submit'
                    )
                # ROUTING LOGIC: Check if this is tool_eval (message-level analysis)
                if prompt_type == 'tool_eval':
                    # Use message-level analysis with N8N UDF integration
                    print(f"    🎯 Using message-level analysis for tool_eval (batch mode)")
                    prompt_results = analyze_messages_with_tool_eval_prompt(
                        session, conversations_df, department_name, prompt_type,
                        prompt_config, target_date, execution_mode='batch_submit'
                    )
                else:
                    # Use conversation-level analysis (existing flow)
                    prompt_results = analyze_conversations_with_prompt(
                        session, conversations_df, department_name, prompt_type, 
                        prompt_config, target_date, execution_mode='batch_submit'
                    )
                
                batch_results[prompt_type] = prompt_results
                
                # Log submission results
                if prompt_results.get('status') == 'submitted':
                    batch_id = prompt_results.get('batch_id')
                    total_requests = prompt_results.get('total_requests', 0)
                    print(f"    ✅ Batch submitted: {batch_id} ({total_requests} requests)")
                else:
                    error = prompt_results.get('error', 'Unknown error')
                    print(f"    ❌ Batch submission failed: {error}")
                    # Continue with next prompt (as per user requirements)
            
            except Exception as e:
                error_msg = f"Failed to submit batch for {prompt_type}: {str(e)}"
                print(f"    ❌ {error_msg}")
                batch_results[prompt_type] = {'error': error_msg, 'status': 'failed'}
                # Continue with next prompt (as per user requirements)
        
        # Summary
        total_prompts = len(batch_results)
        submitted_batches = sum(1 for result in batch_results.values() 
                               if result.get('status') == 'submitted')
        failed_batches = sum(1 for result in batch_results.values() 
                            if result.get('status') == 'failed')
        
        print(f"\n✅ BATCH PHASE 1 COMPLETED - {department_name}:")
        print(f"   📤 Batches submitted: {submitted_batches}/{total_prompts}")
        print(f"   ❌ Batches failed: {failed_batches}/{total_prompts}")
        
        return batch_results, True
    
    except Exception as e:
        error_msg = f"Batch Phase 1 failed: {str(e)}"
        error_details = format_error_details(e, f"BATCH PHASE 1 - {department_name}")
        print(f"  ❌ {department_name}: {error_msg}")
        print(error_details)
        return {'error': error_msg}, False


def process_department_llm_analysis_batch_phase2(session: snowpark.Session, department_name, target_date=None, selected_prompts=None):
    """
    PHASE 2: Fetch completed batches and continue post-processing
    
    This function:
    - Fetches batch records from BATCH_TRACKING table
    - For each batch, checks status and fetches results if completed
    - Marks timeouts (>6 hours) and continues to next batch
    - Returns fetch results
    
    Args:
        session: Snowflake session
        department_name: Department name
        target_date: Target date
        selected_prompts: List of prompt types to process (default: all)
    
    Returns:
        Tuple: (fetch_results, success)
    """
    print(f"\n📥 BATCH PHASE 2: FETCH RESULTS - {department_name}")
    print("=" * 60)
    
    try:
        # Step 1: Fetch batch tracking records
        print(f"📊 Step 1: Fetching batch tracking records...")
        target_date_str = target_date if target_date else datetime.now().strftime("%Y-%m-%d")
        
        batch_records = fetch_batch_tracking_records(
            session, target_date_str, department=department_name
        )
        
        if not batch_records:
            print(f"    ⚠️  No batch records found for {department_name} on {target_date_str}")
            return {}, True
        
        # Filter by selected_prompts if specified
        if selected_prompts and selected_prompts != ['*']:
            selected_set = set(selected_prompts)
            batch_records = [r for r in batch_records if r['PROMPT_TYPE'] in selected_set]
        
        print(f"    ✅ Found {len(batch_records)} batch records")
        
        # Step 2: Fetch results for each batch
        fetch_results = {}
        
        for record in batch_records:
            prompt_type = record['PROMPT_TYPE']
            batch_id = record['BATCH_ID']
            output_table = record['OUTPUT_TABLE']
            model_name = record.get('MODEL_NAME', 'gpt-5-mini')  # Get model name from tracking table
            
            print(f"\n📥 [{prompt_type}] Fetching batch results...")
            print(f"    Batch ID: {batch_id}")
            print(f"    Model: {model_name}")
            
            try:
                # Try to fetch the batch results
                fetch_success, processed_count, failed_count, batch_status = run_batch_llm_fetch(
                    session, batch_id, output_table, department_name, target_date_str, model_name, prompt_type
                )
                
                if fetch_success:
                    # Batch completed successfully (or cancelled with partial results)
                    # Use the actual status returned from the API
                    if batch_status == 'cancelled':
                        result_status = 'cancelled_with_results'
                        print(f"    ⚠️  Batch was cancelled - fetched {processed_count} partial results, {failed_count} failed")
                    else:
                        result_status = 'completed'
                        print(f"    ✅ Batch completed: {processed_count} processed, {failed_count} failed")
                    
                    fetch_results[prompt_type] = {
                        'status': result_status,
                        'batch_id': batch_id,
                        'processed_count': processed_count,
                        'failed_count': failed_count,
                        'total_requests': record['TOTAL_REQUESTS']
                    }
                    
                    # Update BATCH_TRACKING table with appropriate status
                    batch_info = {
                        'PROMPT_TYPE': prompt_type,
                        'BATCH_ID': batch_id,
                        'BATCH_STATUS': result_status,
                        'INPUT_FILE_ID': record.get('INPUT_FILE_ID', ''),
                        'OUTPUT_TABLE': output_table,
                        'MODEL_NAME': record.get('MODEL_NAME', ''),
                        'CONVERSION_TYPE': record.get('CONVERSION_TYPE', ''),
                        'TOTAL_REQUESTS': record['TOTAL_REQUESTS'],
                        'ERROR_MESSAGE': 'Partial results from cancelled batch' if result_status == 'cancelled_with_results' else ''
                    }
                    update_batch_tracking(session, target_date_str, department_name, prompt_type, batch_info)
                else:
                    # Batch not completed or timed out
                    fetch_results[prompt_type] = {
                        'status': 'timeout',
                        'batch_id': batch_id,
                        'error': 'Batch not completed after 6 hours (timeout)',
                        'total_requests': record['TOTAL_REQUESTS']
                    }
                    print(f"    ⏳ Batch timeout: Not completed after 6 hours")
                    
                    # Update BATCH_TRACKING table to mark as timeout
                    batch_info = {
                        'PROMPT_TYPE': prompt_type,
                        'BATCH_ID': batch_id,
                        'BATCH_STATUS': 'timeout',
                        'INPUT_FILE_ID': record.get('INPUT_FILE_ID', ''),
                        'OUTPUT_TABLE': output_table,
                        'MODEL_NAME': record.get('MODEL_NAME', ''),
                        'CONVERSION_TYPE': record.get('CONVERSION_TYPE', ''),
                        'TOTAL_REQUESTS': record['TOTAL_REQUESTS'],
                        'ERROR_MESSAGE': 'Batch not completed after 6 hours'
                    }
                    update_batch_tracking(session, target_date_str, department_name, prompt_type, batch_info)
                    # Continue with next batch (as per user requirements)
            
            except Exception as e:
                error_msg = f"Failed to fetch batch for {prompt_type}: {str(e)}"
                print(f"    ❌ {error_msg}")
                fetch_results[prompt_type] = {
                    'status': 'failed',
                    'batch_id': batch_id,
                    'error': error_msg
                }
                
                # Update BATCH_TRACKING table to mark as failed
                batch_info = {
                    'PROMPT_TYPE': prompt_type,
                    'BATCH_ID': batch_id,
                    'BATCH_STATUS': 'failed',
                    'INPUT_FILE_ID': record.get('INPUT_FILE_ID', ''),
                    'OUTPUT_TABLE': output_table,
                    'MODEL_NAME': record.get('MODEL_NAME', ''),
                    'CONVERSION_TYPE': record.get('CONVERSION_TYPE', ''),
                    'TOTAL_REQUESTS': record['TOTAL_REQUESTS'],
                    'ERROR_MESSAGE': error_msg
                }
                update_batch_tracking(session, target_date_str, department_name, prompt_type, batch_info)
                # Continue with next batch (as per user requirements)
        
        # Summary
        total_batches = len(fetch_results)
        completed_batches = sum(1 for result in fetch_results.values() 
                               if result.get('status') in ['completed', 'cancelled_with_results'])
        cancelled_batches = sum(1 for result in fetch_results.values() 
                               if result.get('status') == 'cancelled_with_results')
        timeout_batches = sum(1 for result in fetch_results.values() 
                             if result.get('status') == 'timeout')
        failed_batches = sum(1 for result in fetch_results.values() 
                            if result.get('status') == 'failed')
        
        print(f"\n✅ BATCH PHASE 2 COMPLETED - {department_name}:")
        print(f"   📥 Batches completed: {completed_batches}/{total_batches}")
        if cancelled_batches > 0:
            print(f"   ⚠️  Batches cancelled (with partial results): {cancelled_batches}/{total_batches}")
        print(f"   ⏳ Batches timed out: {timeout_batches}/{total_batches}")
        print(f"   ❌ Batches failed: {failed_batches}/{total_batches}")
        
        return fetch_results, True
    
    except Exception as e:
        error_msg = f"Batch Phase 2 failed: {str(e)}"
        error_details = format_error_details(e, f"BATCH PHASE 2 - {department_name}")
        print(f"  ❌ {department_name}: {error_msg}")
        print(error_details)
        return {'error': error_msg}, False


def run_batch_llm_cancel(session: snowpark.Session, batch_id, department_name, model_name=None, prompt_type=None):
    """
    Cancel a batch job via OpenAI/Gemini Batch API
    
    Args:
        session: Snowflake session
        batch_id: Batch ID (OpenAI or Gemini)
        department_name: Department name
        model_name: Model name (e.g., 'gpt-5-mini', 'gemini-2.5-flash') to determine LLM function
        prompt_type: Prompt type (e.g., 'tool_eval', 'SA_prompt', etc.)
    
    Returns:
        Tuple: (success: bool, status: str)
    """
    try:
        # Determine model_type and llm_function from model_name AND prompt_type
        if model_name:
            if 'gemini' in model_name.lower():
                model_type = 'gemini'
                llm_function = 'gemini_chat_system'
            else:
                model_type = 'openai'
                # For tool_eval, use Chat Completions API
                if prompt_type == 'tool_eval':
                    llm_function = 'openai_chat_system_completions'
                else:
                    llm_function = 'openai_chat_system'
        else:
            # Default to OpenAI for backward compatibility
            model_type = 'openai'
            llm_function = 'openai_chat_system'
        
        print(f"    🚫 Cancelling {model_type.upper()} batch {batch_id}...")
        
        # Call the UDF to cancel the batch
        cancel_sql = f"""
        WITH r AS (
        SELECT {llm_function}(
                NULL, NULL,
                '{model_name or 'gpt-5-mini'}', 0, 0, 'minimal',
                '{department_name}',
                'batch_cancel',
                NULL,
                '{batch_id}'
                ) AS v
        )
        SELECT
        v:status::string AS STATUS,
        v:batch_id::string AS BATCH_ID
        FROM r
        """
        
        cancel_row = session.sql(cancel_sql).collect()[0]
        status = (cancel_row['STATUS'] or '').lower()
        
        print(f"    ✅ Batch cancellation initiated: {status}")
        
        return True, status
    
    except Exception as e:
        error_msg = f"Batch cancellation failed: {str(e)}"
        print(f"    ❌ {error_msg}")
        print(format_error_details(e, f"BATCH CANCEL - {batch_id}"))
        return False, 'error'


def process_department_llm_analysis_batch_cancel(session: snowpark.Session, department_name, target_date=None, selected_prompts=None):
    """
    BATCH CANCEL: Cancel all in-progress batches for a department
    
    This function:
    - Fetches batch records from BATCH_TRACKING table
    - Filters for batches that are still in-progress (not completed, not failed, not cancelled)
    - Cancels each in-progress batch
    - Updates BATCH_TRACKING table with cancelled status
    - Returns cancellation results
    
    Args:
        session: Snowflake session
        department_name: Department name
        target_date: Target date
        selected_prompts: List of prompt types to process (default: all)
    
    Returns:
        Tuple: (cancel_results, success)
    """
    print(f"\n🚫 BATCH CANCEL: CANCEL IN-PROGRESS BATCHES - {department_name}")
    print("=" * 60)
    
    try:
        # Step 1: Fetch batch tracking records
        print(f"📊 Step 1: Fetching batch tracking records...")
        target_date_str = target_date if target_date else datetime.now().strftime("%Y-%m-%d")
        
        batch_records = fetch_batch_tracking_records(
            session, target_date_str, department=department_name
        )
        
        if not batch_records:
            print(f"    ⚠️  No batch records found for {department_name} on {target_date_str}")
            return {}, True
        
        # Filter by selected_prompts if specified
        if selected_prompts and selected_prompts != ['*']:
            selected_set = set(selected_prompts)
            batch_records = [r for r in batch_records if r['PROMPT_TYPE'] in selected_set]
        
        print(f"    ✅ Found {len(batch_records)} batch records")
        
        # Step 2: Filter for in-progress batches only
        # In-progress means: status is 'submitted', 'validating', 'in_progress', or similar (not completed/failed/cancelled)
        terminal_statuses = {'completed', 'failed', 'cancelled', 'cancelled_with_results', 'expired'}
        in_progress_records = [r for r in batch_records if r['BATCH_STATUS'].lower() not in terminal_statuses]
        
        print(f"    🔍 Found {len(in_progress_records)} in-progress batches to cancel")
        
        if not in_progress_records:
            print(f"    ℹ️  No in-progress batches found - nothing to cancel")
            return {}, True
        
        # Step 3: Cancel each in-progress batch
        cancel_results = {}
        
        for record in in_progress_records:
            prompt_type = record['PROMPT_TYPE']
            batch_id = record['BATCH_ID']
            batch_status = record['BATCH_STATUS']
            output_table = record['OUTPUT_TABLE']
            model_name = record.get('MODEL_NAME', 'gpt-5-mini')
            
            print(f"\n🚫 [{prompt_type}] Cancelling batch...")
            print(f"    Batch ID: {batch_id}")
            print(f"    Current Status: {batch_status}")
            print(f"    Model: {model_name}")
            
            try:
                # Cancel the batch
                cancel_success, new_status = run_batch_llm_cancel(
                    session, batch_id, department_name, model_name, prompt_type
                )
                
                if cancel_success:
                    print(f"    ✅ Batch cancelled successfully: {new_status}")
                    
                    cancel_results[prompt_type] = {
                        'status': 'cancelled',
                        'batch_id': batch_id,
                        'previous_status': batch_status
                    }
                    
                    # Update BATCH_TRACKING table to mark as cancelled
                    batch_info = {
                        'PROMPT_TYPE': prompt_type,
                        'BATCH_ID': batch_id,
                        'BATCH_STATUS': 'cancelled',
                        'INPUT_FILE_ID': record.get('INPUT_FILE_ID', ''),
                        'OUTPUT_TABLE': output_table,
                        'MODEL_NAME': record.get('MODEL_NAME', ''),
                        'CONVERSION_TYPE': record.get('CONVERSION_TYPE', ''),
                        'TOTAL_REQUESTS': record['TOTAL_REQUESTS'],
                        'ERROR_MESSAGE': f'Manually cancelled (was {batch_status})'
                    }
                    update_batch_tracking(session, target_date_str, department_name, prompt_type, batch_info)
                else:
                    print(f"    ❌ Batch cancellation failed")
                    cancel_results[prompt_type] = {
                        'status': 'failed',
                        'batch_id': batch_id,
                        'error': 'Cancellation failed'
                    }
            
            except Exception as e:
                error_msg = f"Failed to cancel batch for {prompt_type}: {str(e)}"
                print(f"    ❌ {error_msg}")
                cancel_results[prompt_type] = {
                    'status': 'failed',
                    'batch_id': batch_id,
                    'error': error_msg
                }
        
        # Summary
        total_batches = len(cancel_results)
        cancelled_batches = sum(1 for result in cancel_results.values() 
                               if result.get('status') == 'cancelled')
        failed_cancels = sum(1 for result in cancel_results.values() 
                            if result.get('status') == 'failed')
        
        print(f"\n✅ BATCH CANCEL COMPLETED - {department_name}:")
        print(f"   🚫 Batches cancelled: {cancelled_batches}/{total_batches}")
        print(f"   ❌ Failed cancellations: {failed_cancels}/{total_batches}")
        
        return cancel_results, True
    
    except Exception as e:
        error_msg = f"Batch Cancel failed: {str(e)}"
        error_details = format_error_details(e, f"BATCH CANCEL - {department_name}")
        print(f"  ❌ {department_name}: {error_msg}")
        print(error_details)
        return {'error': error_msg}, False


def update_department_master_summary(session: snowpark.Session, department_name, target_date, selected_metrics=None):
    """
    Calculate metrics and update department-specific master summary table
    
    Args:
        session: Snowflake session
        department_name: Department name to process
        target_date: Target date for analysis
    
    Returns:
        Success status and summary of metrics calculated
    """
    print(f"📊 UPDATING DEPARTMENT MASTER SUMMARY: {department_name}")
    
    try:
        # Get department's metrics configuration
        metrics_config = get_metrics_configuration()
        
        if department_name not in metrics_config:
            print(f"   ⚠️  No metrics configuration found for {department_name}")
            return True, {'warning': f'No metrics configuration for {department_name}'}
        
        dept_config = metrics_config[department_name]
        master_table = dept_config['master_table']
        dept_metrics = dept_config['metrics']
        # Filter metrics if a subset was requested
        if selected_metrics is not None and selected_metrics != ['*']:
            selected_set = set(selected_metrics)
            missing_metrics = [m for m in selected_metrics if m not in dept_metrics]
            if missing_metrics:
                print(f"   ⚠️  Skipping unknown metrics for {department_name}: {missing_metrics}")
            dept_metrics = {k: v for k, v in dept_metrics.items() if k in selected_set}
        
        print(f"   🎯 Processing {len(dept_metrics)} metrics for {department_name}")
        
        # Check if there are any metrics to process after filtering
        if not dept_metrics:
            print(f"   ⚠️  No metrics to process for {department_name} - skipping metrics calculation")
            return True, {'warning': f'No metrics to process for {department_name}'}
        
        # Get department's configured prompts to check dependencies
        departments_config = get_snowflake_llm_departments_config()
        configured_prompts = list(departments_config.get(department_name, {}).get('llm_prompts', {}).keys())
        
        # Calculate metrics in order
        metric_results = {}
        successful_metrics = 0
        failed_metrics = 0
        
        for metric_name, metric_config in sorted(dept_metrics.items(), key=lambda x: x[1]['order']):
            print(f"   📈 Calculating {metric_name}...")
            
            # Check if required prompts are configured for this department
            required_prompts = metric_config['depends_on_prompts']
            missing_prompts = [p for p in required_prompts if p not in configured_prompts]
            
            # if missing_prompts:
            #     print(f"     ⚠️  Missing required prompts {missing_prompts} - setting NULL")
            #     # Set NULL values for missing dependencies
            #     for col in metric_config['columns']:
            #         metric_results[col] = None
            #     failed_metrics += 1
            #     continue
            
            try:
                # Get the function object directly from configuration
                calc_function = metric_config['function']
                
                # Validate that it's actually a callable function
                if not callable(calc_function):
                    print(f"     ❌ Function {calc_function} is not callable")
                    # Set NULL values for invalid function
                    for col in metric_config['columns']:
                        metric_results[col] = None
                    failed_metrics += 1
                    continue
                
                # Call the function (all functions take session and target_date)
                result = calc_function(session, department_name, target_date)
                
                # Map results to column names
                columns = metric_config['columns']
                if isinstance(result, tuple):
                    # Multiple return values
                    for i, col in enumerate(columns):
                        metric_results[col] = result[i] if i < len(result) else None
                elif isinstance(result, dict):
                    # Dictionary return (for weighted NPS)
                    if len(columns) == 1 and department_name in result:
                        metric_results[columns[0]] = result[department_name]
                    else:
                        # Set NULL if department not found in result
                        for col in columns:
                            metric_results[col] = None
                else:
                    # Single return value
                    if len(columns) == 1:
                        metric_results[columns[0]] = result
                    else:
                        print(f"     ⚠️  Mismatch: function returned single value but {len(columns)} columns expected")
                        metric_results[columns[0]] = result
                        for col in columns[1:]:
                            metric_results[col] = None
                
                successful_metrics += 1
                print(f"     ✅ {metric_name} calculated successfully")
                
            except Exception as e:
                print(f"     ❌ Error calculating {metric_name}: {str(e)}")
                # Set NULL values for failed calculation
                for col in metric_config['columns']:
                    metric_results[col] = None
                failed_metrics += 1
                continue

        # ------------------------------------------------------------------
        # Backwards-compatible aliases (schema drift safety)
        # ------------------------------------------------------------------
        # Some Snowflake summary tables historically used *_PERCENTAGE_X/Y and *_DENOMINATOR_D/_d
        # for the Transfers Due To Escalations metric. Populate both variants when present so
        # downstream queries (and older tables) stay consistent.
        if 'TRANSFERS_DUE_TO_ESCALATIONS_PERCENTAGE' in metric_results:
            metric_results.setdefault(
                'TRANSFERS_DUE_TO_ESCALATIONS_PERCENTAGE_X',
                metric_results['TRANSFERS_DUE_TO_ESCALATIONS_PERCENTAGE'],
            )
        if 'TRANSFERS_DUE_TO_ESCALATIONS_OF_TRANSFERS_PERCENTAGE' in metric_results:
            metric_results.setdefault(
                'TRANSFERS_DUE_TO_ESCALATIONS_PERCENTAGE_Y',
                metric_results['TRANSFERS_DUE_TO_ESCALATIONS_OF_TRANSFERS_PERCENTAGE'],
            )
        if 'TRANSFERS_DUE_TO_ESCALATIONS_DENOMINATOR' in metric_results:
            metric_results.setdefault(
                'TRANSFERS_DUE_TO_ESCALATIONS_DENOMINATOR_D',
                metric_results['TRANSFERS_DUE_TO_ESCALATIONS_DENOMINATOR'],
            )
        if 'TRANSFERS_DUE_TO_ESCALATIONS_TRANSFER_DENOMINATOR' in metric_results:
            metric_results.setdefault(
                'TRANSFERS_DUE_TO_ESCALATIONS_TRANSFER_DENOMINATOR_d',
                metric_results['TRANSFERS_DUE_TO_ESCALATIONS_TRANSFER_DENOMINATOR'],
            )
        
        # Prepare record for insertion
        summary_record = {
            **metric_results,  # All calculated metrics
        }
        
        # Convert to DataFrame
        summary_df = pd.DataFrame([summary_record])
        summary_df = clean_dataframe_for_snowflake(summary_df)
        
        # Get dynamic columns (excluding DATE, DEPARTMENT, TIMESTAMP)
        dynamic_columns = [col for col in summary_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
        
        # Decide full insert vs partial upsert
        if should_use_full_insert(session, master_table, dynamic_columns):
            # Insert using existing pattern
            insert_success = insert_raw_data_with_cleanup(
                session=session,
                table_name=master_table,
                department=department_name,
                target_date=target_date,
                dataframe=summary_df[dynamic_columns],
                columns=dynamic_columns
            )
        else:
            # Partial update/insert
            insert_success = insert_raw_data_partial(
                session=session,
                table_name=master_table,
                department=department_name,
                target_date=target_date,
                values_dict=summary_df.iloc[0].to_dict()
            )
        
        if insert_success:
            print(f"   ✅ Updated {master_table}: {successful_metrics} metrics calculated, {failed_metrics} set to NULL")
            
            summary = {
                'master_table': master_table,
                'total_metrics': len(dept_metrics),
                'successful_metrics': successful_metrics,
                'failed_metrics': failed_metrics,
                'metric_results': metric_results
            }
            return True, summary
        else:
            print(f"   ❌ Failed to insert into {master_table}")
            return False, {'error': f'Failed to insert into {master_table}'}
        
    except Exception as e:
        error_details = format_error_details(e, f"DEPARTMENT MASTER SUMMARY - {department_name}")
        print(f"   ❌ Failed to update department master summary: {str(e)}")
        print(error_details)
        return False, {'error': str(e)}


def update_llm_master_summary(session: snowpark.Session, department_results, target_date, selected_metrics=None):
    """
    Update department-specific master summary tables with calculated metrics
    
    Args:
        session: Snowflake session
        department_results: Dictionary with results for each department
        target_date: Target date for analysis
    
    Returns:
        Success status
    """
    print(f"\n📊 UPDATING DEPARTMENT MASTER SUMMARIES...")
    
    try:
        summary_stats = {
            'total_departments': 0,
            'successful_departments': 0,
            'failed_departments': 0,
            'total_metrics_calculated': 0,
            'total_metrics_failed': 0
        }
        
        # Process each department that had LLM analysis
        for department_name, dept_results in department_results.items():
            if 'error' in dept_results:
                print(f"   ⚠️  Skipping {department_name} - LLM analysis failed: {dept_results['error']}")
                summary_stats['failed_departments'] += 1
                continue
            
            summary_stats['total_departments'] += 1
            
            # Update this department's master summary table
            # selected_metrics is passed via orchestrator for single-department runs; for multi dept keep full
            success, dept_summary = update_department_master_summary(session, department_name, target_date, selected_metrics)
            
            if success:
                summary_stats['successful_departments'] += 1
                if isinstance(dept_summary, dict):
                    summary_stats['total_metrics_calculated'] += dept_summary.get('successful_metrics', 0)
                    summary_stats['total_metrics_failed'] += dept_summary.get('failed_metrics', 0)
                    print(f"   ✅ {department_name}: {dept_summary.get('successful_metrics', 0)} metrics calculated")
            else:
                summary_stats['failed_departments'] += 1
                print(f"   ❌ {department_name}: Failed to update master summary")
        
        # Print overall summary
        print(f"\n📊 MASTER SUMMARY UPDATE COMPLETED:")
        print(f"   🏢 Departments processed: {summary_stats['successful_departments']}/{summary_stats['total_departments']}")
        print(f"   📈 Total metrics calculated: {summary_stats['total_metrics_calculated']}")
        print(f"   ⚠️  Total metrics set to NULL: {summary_stats['total_metrics_failed']}")
        
        if summary_stats['failed_departments'] > 0:
            print(f"   ❌ Failed departments: {summary_stats['failed_departments']}")
        
        return summary_stats['failed_departments'] == 0
        
    except Exception as e:
        error_details = format_error_details(e, "UPDATE DEPARTMENT MASTER SUMMARIES")
        print(f"   ❌ Failed to update master summaries: {str(e)}")
        print(error_details)
        return False


def filter_conversations_by_category(session: snowpark.Session, filtered_df, category_name, department_name, target_date):
    """
    Filter conversations based on categorizing data from DOCTORS_CATEGORIZING_RAW_DATA table
    
    Args:
        session: Snowflake session
        filtered_df: DataFrame with conversations to filter
        category_name: Category name to filter by (e.g., "Insurance Inquiries", "Emergency Medical", etc.)
        department_name: Department name for filtering
        target_date: Target date for filtering
    
    Returns:
        Filtered DataFrame containing only conversations that match the specified category
    """
    print(f"🔍 FILTERING CONVERSATIONS BY CATEGORY: {category_name}")
    print(f"   📊 Input DataFrame: {len(filtered_df)} rows, {filtered_df['CONVERSATION_ID'].nunique()} unique conversations")
    
    try:
        # Step 1: Fetch categorizing data from DOCTORS_CATEGORIZING_RAW_DATA
        categorizing_query = f"""
        SELECT 
            CONVERSATION_ID,
            LLM_RESPONSE
        FROM LLM_EVAL.PUBLIC.DOCTORS_CATEGORIZING_RAW_DATA
        WHERE DEPARTMENT = '{department_name}'
        AND DATE = '{target_date}'
        AND PROCESSING_STATUS = 'COMPLETED'
        AND LLM_RESPONSE IS NOT NULL
        AND LLM_RESPONSE != ''
        """
        
        print(f"   🔄 Fetching categorizing data for {department_name} on {target_date}...")
        categorizing_df = session.sql(categorizing_query).to_pandas()
        
        if categorizing_df.empty:
            print(f"   ⚠️  No categorizing data found for {department_name} on {target_date}")
            return pd.DataFrame()  # Return empty DataFrame
        
        print(f"   📋 Found {len(categorizing_df)} categorizing records")
        
        # Step 2: Parse JSON responses and find conversations with matching category/flags
        matching_conversation_ids = set()
        parsed_count = 0
        error_count = 0
        
        for _, row in categorizing_df.iterrows():
            try:
                conversation_id = row['CONVERSATION_ID']
                llm_response = row['LLM_RESPONSE']
                
                # Parse JSON response
                if isinstance(llm_response, str) and llm_response.strip():
                    # Clean the response if it has extra formatting
                    cleaned_response = llm_response.strip()
                    if cleaned_response.startswith('```json'):
                        cleaned_response = cleaned_response.replace('```json', '').replace('```', '').strip()
                    
                    response_data = json.loads(cleaned_response)

                    # Primary logic: for Doctors flows we filter by explicit flags rather than the 'category' list
                    # - For 'OTC Medication Advice' and 'Clinic Recommendation', include only if their value is affirmative
                    included = False
                    target_keys = ["OTC Medication Advice", "Clinic Recommendation"]

                    if category_name in target_keys:
                        val = response_data.get(category_name)
                        if isinstance(val, str):
                            included = val.strip().lower() in {"yes", "y", "true", "1"}
                        elif isinstance(val, bool):
                            included = val is True
                        elif isinstance(val, (int, float)):
                            included = (val == 1)
                    else:
                        # Fallback legacy behavior: use 'category' array for other category names
                        if 'category' in response_data:
                            categories = response_data['category']
                            if isinstance(categories, str):
                                categories = [categories]
                            if isinstance(categories, list) and category_name in categories:
                                included = True

                    if included:
                        matching_conversation_ids.add(conversation_id)
                    
                    parsed_count += 1
                
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                error_count += 1
                continue
        
        print(f"   ✅ Parsed {parsed_count} responses, {error_count} parsing errors")
        print(f"   🎯 Found {len(matching_conversation_ids)} conversations with category '{category_name}'")
        
        # Step 3: Filter the input DataFrame to only include matching conversations
        if not matching_conversation_ids:
            print(f"   ⚠️  No conversations found with category '{category_name}'")
            return pd.DataFrame()  # Return empty DataFrame
        
        # Filter the DataFrame
        filtered_result = filtered_df[filtered_df['CONVERSATION_ID'].isin(matching_conversation_ids)].copy()
        
        print(f"   📊 Final filtered result: {len(filtered_result)} rows, {filtered_result['CONVERSATION_ID'].nunique()} unique conversations")
        
        # Add category information for reference
        filtered_result['FILTER_CATEGORY'] = category_name
        filtered_result['FILTER_SOURCE'] = 'DOCTORS_CATEGORIZING_RAW_DATA'
        
        return filtered_result
        
    except Exception as e:
        error_details = format_error_details(e, f"FILTER BY CATEGORY - {category_name}")
        print(f"   ❌ Error filtering by category '{category_name}': {str(e)}")
        print(error_details)
        return pd.DataFrame()  # Return empty DataFrame on error


def test_llm_single_prompt(session: snowpark.Session, department_name, prompt_type, target_date=None, sample_size=1):
    """
    Test LLM analysis for a single prompt with a small sample
    
    Args:
        session: Snowflake session
        department_name: Department name
        prompt_type: Specific prompt type to test
        target_date: Target date for analysis
        sample_size: Number of conversations to test
    
    Returns:
        Test results
    """
    print(f"🧪 TESTING LLM ANALYSIS - {department_name} / {prompt_type}")
    print("=" * 60)
    
    try:
        # Get prompt configuration
        prompt_config = get_prompt_config(department_name, prompt_type)
        if not prompt_config:
            print(f"❌ Prompt configuration not found for {department_name}/{prompt_type}")
            return
        
        # Get a small sample of data
        dept_results, success = process_department_llm_analysis(session, department_name, target_date)
        
        if not success:
            print(f"❌ Failed to get sample data for {department_name}")
            return
        
        if prompt_type in dept_results:
            results = dept_results[prompt_type]
            print(f"\n📊 TEST RESULTS:")
            for key, value in results.items():
                print(f"   {key}: {value}")
        else:
            print(f"❌ No results for prompt type {prompt_type}")
            
    except Exception as e:
        error_details = format_error_details(e, f"LLM TEST - {department_name}/{prompt_type}")
        print(error_details)