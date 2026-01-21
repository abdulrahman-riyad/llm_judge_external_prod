"""
XML Conversion Module for LLM Analysis

Converts conversation DataFrames to XML format for LLM processing.
Adapted from LLM_UTILITIES.py to work with pandas DataFrames directly.

ENVIRONMENT AGNOSTIC:
This module uses only pandas and standard Python - no Snowpark dependencies.
It works in all environments:
- Inside Snowflake (via Snowpark)
- Google Colab
- GitHub Actions
- Local development

Input: pandas DataFrame with conversation messages
Output: pandas DataFrame with CONVERSATION_XML column
"""

import pandas as pd
import json
import xml.sax.saxutils as saxutils
from snowflake_llm_config import get_snowflake_llm_departments_config
from snowflake_llm_helpers import get_execution_id_map, get_tool_name_and_response


def format_tool_with_name_as_xml(tool_name, tool_output, tool_time):
    """
    Convert a tool invocation to XML format.

    IMPORTANT: include the tool args payload in `<o>` so downstream LLM prompts can
    validate correct tool usage (e.g., `send_document` document_names).
    """
    escaped_tool_name = saxutils.escape(str(tool_name))
    escaped_tool_time = saxutils.escape(str(tool_time))

    # Normalize tool args/output into a stable dict
    args_obj = {}
    try:
        if isinstance(tool_output, dict):
            args_obj = tool_output
        elif isinstance(tool_output, str):
            s = tool_output.strip()
            if s:
                try:
                    parsed = json.loads(s)
                    args_obj = parsed if isinstance(parsed, dict) else {"raw": tool_output}
                except Exception:
                    args_obj = {"raw": tool_output}
        elif tool_output is None:
            args_obj = {}
        else:
            args_obj = {"raw": str(tool_output)}
    except Exception:
        args_obj = {}

    # Mirror the system `tool_calls` format used in prompts
    envelope = {"tool_calls": [{"type": "tool_call", "name": str(tool_name), "args": args_obj}]}
    try:
        out_str = f"[SYSTEM: {json.dumps(envelope, ensure_ascii=False)}]"
    except Exception:
        out_str = "[SYSTEM: {\"tool_calls\":[]}]"

    escaped_output = saxutils.escape(out_str)
    return f"<tool>\n  <n>{escaped_tool_name}</n>\n  <t>{escaped_tool_time}</t>\n  <o>{escaped_output}</o>\n</tool>"


def preprocess_conversation_dataframe(conv_df):
    """
    Preprocess a single conversation DataFrame
    Sort by message timestamp - works directly with Snowflake column names
    """
    # Sort by MESSAGE_SENT_TIME (Snowflake column name)
    if 'MESSAGE_SENT_TIME' in conv_df.columns:
        conv_df = conv_df.sort_values('MESSAGE_SENT_TIME')
    
    return conv_df


def convert_single_conversation_to_xml(conv_df, department_name, include_tool_messages=True, include_time_stamps = False, debug_info=None):
    """
    Convert a single conversation DataFrame to XML format
    Adapted from LLM_UTILITIES.py convert_conversation_to_xml()
    
    Args:
        conv_df: DataFrame containing messages for one conversation
        department_name: Department name for skill filtering
    
    Returns:
        XML string representation of the conversation
    """
    
    # Filter conv_df to only include specific message types
    conv_df = conv_df[conv_df['MESSAGE_TYPE'].str.lower().isin(['tool', 'tool response', 'normal message'])]
    
    if conv_df.empty:
        return None
        
    # Get department configuration
    departments_config = get_snowflake_llm_departments_config()
    if department_name not in departments_config:
        if isinstance(debug_info, dict):
            debug_info['reason'] = 'department_not_configured'
        return None
        
    dept_config = departments_config[department_name]
    target_bot_skills = dept_config['bot_skills']
    target_agent_skills = dept_config['agent_skills']
    
    # Preprocess the conversation
    conv_df = preprocess_conversation_dataframe(conv_df.copy())
    
    # Check required columns exist (using Snowflake column names)
    required_columns = ['CONVERSATION_ID', 'MESSAGE_SENT_TIME', 'SENT_BY', 'TEXT']
    missing_columns = [col for col in required_columns if col not in conv_df.columns]
    if missing_columns:
        if isinstance(debug_info, dict):
            debug_info['reason'] = 'missing_columns'
            debug_info['details'] = {'missing_columns': missing_columns}
        return None
    
    # Check if conversation contains target skills
    if 'TARGET_SKILL_PER_MESSAGE' in conv_df.columns:
        skills_series = conv_df['TARGET_SKILL_PER_MESSAGE'].fillna('')
        skills = list(set(skills_series.tolist()))
        if not any(skill in target_bot_skills for skill in skills):
            if isinstance(debug_info, dict):
                debug_info['reason'] = 'no_target_skill_match'
                debug_info['details'] = {'skills_found': skills, 'required_bot_skills': target_bot_skills}
            return None
    
    # Get unique participants
    sent_by_series = conv_df['SENT_BY'].fillna('')
    participants = sorted(list(set(sent_by_series.tolist())))
    
    # Check if any participant is 'bot' or 'agent' (case-insensitive)
    has_bot_or_agent = any(p.lower() in ["bot", "agent"] for p in participants)
    has_consumer = any(p.lower() == "consumer" for p in participants)
    if not has_bot_or_agent or not has_consumer:
        if isinstance(debug_info, dict):
            debug_info['reason'] = 'participants_missing'
            debug_info['details'] = {'participants': participants}
        return None
    
    # Get conversation ID
    conv_id = conv_df['CONVERSATION_ID'].iloc[0] if len(conv_df) > 0 else "unknown"
    
    # Start building XML content
    content_parts = []
    
    # Track last message details for duplicate detection
    last_message_time = None
    last_message_text = None
    last_message_sender = None
    last_message_type = None
    
    # Track the last skill seen in the conversation
    last_skill = ""

    # Find starting index - first row where skill is in target_bot_skills
    # OR first row where skill is null but next non-null skill is in target_bot_skills
    # start_idx = None
    
    # for i, (idx, row) in enumerate(conv_df.iterrows()):
    #     skill_value = row.get('TARGET_SKILL_PER_MESSAGE', '')
    #     current_skill = str(skill_value) if pd.notna(skill_value) else ""
        
    #     # Check if current skill is in target_bot_skills
    #     if current_skill and current_skill != "nan" and current_skill != "None" and current_skill in target_bot_skills:
    #         start_idx = idx
    #         break
        
    #     # If current skill is null/empty, look ahead for next non-null skill
    #     if not current_skill or current_skill == "" or current_skill == "nan" or current_skill == "None":
    #         # Look ahead to find the next non-null skill
    #         for j, (future_idx, future_row) in enumerate(conv_df.iloc[i+1:].iterrows(), start=i+1):
    #             future_skill_value = future_row.get('TARGET_SKILL_PER_MESSAGE', '')
    #             future_skill = str(future_skill_value) if pd.notna(future_skill_value) else ""
                
    #             # Found a non-null skill
    #             if future_skill and future_skill != "" and future_skill != "nan" and future_skill != "None":
    #                 if future_skill in target_bot_skills:
    #                     start_idx = idx  # Use current row (with null skill) as start
    #                 break  # Stop looking ahead (whether match or not)
            
    #         if start_idx is not None:
    #             break  # Found our starting point

    # if start_idx is None:
    #     if isinstance(debug_info, dict):
    #         debug_info['reason'] = 'no_starting_skill_match'
    #     return None
    
    # Process each message
    for _, row in conv_df.iterrows():
        # Get message time (using Snowflake column name)
        message_time = row['MESSAGE_SENT_TIME']
        if pd.notna(message_time):
            current_time = str(message_time)
        else:
            current_time = ""
        
        # Get message text
        text_value = row['TEXT']
        current_text = str(text_value) if pd.notna(text_value) else ""
        
        # Get sender (using Snowflake column name)
        sent_by_value = row['SENT_BY']
        current_sender = str(sent_by_value) if pd.notna(sent_by_value) else ""
        
        # Get skill (using Snowflake column name)
        skill_value = row.get('TARGET_SKILL_PER_MESSAGE', '')
        current_skill = str(skill_value) if pd.notna(skill_value) else ""
        
        
        # Get message type (using Snowflake column name)
        message_type_value = row.get('MESSAGE_TYPE', '')
        current_type = str(message_type_value).lower() if pd.notna(message_type_value) else ""

        # Skip transfer and private messages
        if current_type == "transfer" or current_type == "private message" or current_type == "tool response":
            continue

        
        # Handle bot messages from non-target skills
        if current_sender.lower() == "bot" and current_skill not in target_bot_skills:
            # comment later
            # break
            current_sender = "Agent_1"


        # Handle empty messages
        if (current_text == "" or pd.isna(text_value)) and current_type == "normal message":
            current_text = "[Doc/Image]"

        # Check if this is a duplicate message (same time, text, sender, and type)
        is_duplicate = (current_time == last_message_time and 
                      current_text == last_message_text and 
                      current_sender == last_message_sender and 
                      current_type == last_message_type)
        
        # Update last skill if we have a skill value and this is not a duplicate
        if current_skill and not is_duplicate and current_skill != "nan":
            last_skill = current_skill
        
        # Add tool message by resolving tool name and matching tool response content
        if current_type == "tool" and current_text:
            if current_skill not in target_bot_skills:
                continue
            if include_tool_messages:
                tool_time = row.get('MESSAGE_SENT_TIME', '')
                
                tool_name, tool_output, tool_args = get_tool_name_and_response(conv_df, text_value)
                tool_payload = tool_args if tool_args is not None else (tool_output if tool_output is not None else {})
                tool_xml = format_tool_with_name_as_xml(tool_name or "Unknown_Tool", tool_payload, tool_time)
                content_parts.append(tool_xml)
            continue
            
        
        # If there's a message and it's not a duplicate, add it
        if current_text and not is_duplicate:
            # Escape XML special characters in the message content
            escaped_text = saxutils.escape(current_text)
            
            # Special formatting for system messages
            if current_sender.lower() == "system":
                message_line = f"[SYSTEM: {escaped_text}]"
            elif include_time_stamps:
                message_line = f"{current_sender} ({current_time}): {escaped_text}"
            else:
                message_line = f"{current_sender}: {escaped_text}"
            
            content_parts.append(message_line)
            
            # Update last message details
            last_message_time = current_time
            last_message_text = current_text
            last_message_sender = current_sender
            last_message_type = current_type

        # if last_message_sender.lower() == "agent":
        #     break
    
    # Only proceed if we have content
    if not content_parts:
        if isinstance(debug_info, dict):
            debug_info['reason'] = 'no_content_after_cleaning'
        return None
    
    # Join all content parts with newlines
    content_xml = "\n\n".join(content_parts)
    
    # Build the full XML structure
    full_xml = f"""<conversation>
    <chatID>{saxutils.escape(str(conv_id))}</chatID>
    <content>

    {content_xml}

    </content>
    </conversation>"""
    
    return full_xml


def convert_conversations_to_xml_dataframe(filtered_df, department_name, include_tool_messages=True, include_time_stamps=False):
    """
    Convert filtered DataFrame conversations to XML format without saving CSV files.
    
    Args:
        filtered_df: Filtered DataFrame from Phase 1 processing
        department_name: Department name for configuration
    
    Returns:
        DataFrame with columns: conversation_id, content_xml_view, department, last_skill
    """
    print(f"    🔄 Converting conversations to XML format for {department_name}...")
    
    if filtered_df.empty:
        print(f"    ⚠️  No conversations to convert")
        return pd.DataFrame(columns=['conversation_id', 'content_xml_view', 'department', 'last_skill'])
    
    xml_conversations = []
    processed_count = 0

    # Debug counters
    total_conversations = 0
    dropped_missing_columns = 0
    dropped_no_target_skill = 0
    dropped_participants_missing = 0
    dropped_no_content = 0
    dropped_other = 0
    
    # Group by conversation ID and process each conversation (using Snowflake column name)
    if 'CONVERSATION_ID' not in filtered_df.columns:
        print(f"    ❌ CONVERSATION_ID column not found in DataFrame")
        return pd.DataFrame(columns=['conversation_id', 'content_xml_view', 'department', 'last_skill'])
    
    execution_id_map = get_execution_id_map(filtered_df, department_name)

    for conv_id, conv_df in filtered_df.groupby('CONVERSATION_ID'):
        total_conversations += 1
        # Convert conversation to XML with debug capture
        drop_info = {}
        xml_content = convert_single_conversation_to_xml(conv_df, department_name, include_tool_messages, include_time_stamps, debug_info=drop_info)
        
        if xml_content:
            # Extract ID columns for user type determination
            client_id = conv_df['CLIENT_ID'].iloc[0] if 'CLIENT_ID' in conv_df.columns and pd.notna(conv_df['CLIENT_ID'].iloc[0]) else None
            maid_id = conv_df['MAID_ID'].iloc[0] if 'MAID_ID' in conv_df.columns and pd.notna(conv_df['MAID_ID'].iloc[0]) else None
            applicant_id = conv_df['APPLICANT_ID'].iloc[0] if 'APPLICANT_ID' in conv_df.columns and pd.notna(conv_df['APPLICANT_ID'].iloc[0]) else None
            
            # Determine USER_TYPE: 'maid' if maid_id or applicant_id exists, otherwise 'client'
            user_type = 'maid' if (maid_id is not None or applicant_id is not None) else 'client'
            
            
            xml_conversations.append({
                'conversation_id': str(conv_id),
                'content_xml_view': xml_content,
                'department': department_name,
                'last_skill': conv_df['SKILL'].iloc[0],
                'execution_id': execution_id_map.get(conv_id, ''),
                'agent_names': ", ".join(sorted(set(conv_df['AGENT_NAME'].dropna().astype(str)))) if 'AGENT_NAME' in conv_df.columns else "",
                'shadowed_by': conv_df['SHADOWED_BY'].iloc[0],
                'customer_name': (conv_df['CUSTOMER_NAME'].dropna().astype(str).iloc[0] if ('CUSTOMER_NAME' in conv_df.columns and not conv_df['CUSTOMER_NAME'].dropna().empty) else ""),
                'user_type': user_type,
            })
            processed_count += 1
        else:
            # Count and log dropped conversations with reasons
            reason = drop_info.get('reason', 'unknown')
            details = drop_info.get('details', {})
            if reason == 'missing_columns':
                dropped_missing_columns += 1
            elif reason == 'no_target_skill_match':
                dropped_no_target_skill += 1
            elif reason == 'participants_missing':
                dropped_participants_missing += 1
            elif reason == 'no_content_after_cleaning':
                dropped_no_content += 1
            else:
                dropped_other += 1
            # print(f"    🛈 Dropped conversation {conv_id} during XML conversion | reason={reason} | details={details}")
    
    print(f"    ✅ Converted {processed_count} conversations to XML")

    # Print debug summary
    print(f"    ─ Debug: XML conversion funnel ─")
    print(f"      • Total conversations before XML: {total_conversations}")
    print(f"      • Dropped (missing columns): {dropped_missing_columns}")
    print(f"      • Dropped (no per-message bot skill match): {dropped_no_target_skill}")
    print(f"      • Dropped (participants missing): {dropped_participants_missing}")
    print(f"      • Dropped (no content after cleaning): {dropped_no_content}")
    print(f"      • Dropped (other): {dropped_other}")
    print(f"      • Final converted: {processed_count}")
    
    return pd.DataFrame(xml_conversations)


def validate_xml_conversion(xml_conversations_df, department_name):
    """
    Validate the XML conversion results
    
    Args:
        xml_conversations_df: DataFrame with XML conversations
        department_name: Department name
    
    Returns:
        Validation results dictionary
    """
    validation_results = {
        'total_conversations': len(xml_conversations_df),
        'valid_xml_count': 0,
        'empty_xml_count': 0,
        'errors': []
    }
    
    if xml_conversations_df.empty:
        validation_results['errors'].append("No conversations in XML DataFrame")
        return validation_results
    
    # Check each XML conversation
    for _, row in xml_conversations_df.iterrows():
        xml_content = row.get('content_xml_view', '')
        
        if not xml_content or xml_content.strip() == '':
            validation_results['empty_xml_count'] += 1
        else:
            # Basic XML structure validation
            if '<conversation>' in xml_content and '</conversation>' in xml_content:
                if '<chatID>' in xml_content and '<content>' in xml_content:
                    validation_results['valid_xml_count'] += 1
                else:
                    validation_results['errors'].append(f"Missing XML elements in conversation {row.get('conversation_id', 'unknown')}")
            else:
                validation_results['errors'].append(f"Invalid XML structure in conversation {row.get('conversation_id', 'unknown')}")
    
    validation_results['success_rate'] = (
        validation_results['valid_xml_count'] / validation_results['total_conversations'] * 100
    ) if validation_results['total_conversations'] > 0 else 0
    
    return validation_results


def get_xml_sample(xml_conversations_df, sample_size=1):
    """
    Get a sample of XML conversations for inspection
    
    Args:
        xml_conversations_df: DataFrame with XML conversations
        sample_size: Number of samples to return
    
    Returns:
        Sample XML conversations
    """
    if xml_conversations_df.empty:
        return []
    
    sample_df = xml_conversations_df.head(sample_size)
    samples = []
    
    for _, row in sample_df.iterrows():
        samples.append({
            'conversation_id': row.get('conversation_id', 'unknown'),
            'department': row.get('department', 'unknown'),
            'last_skill': row.get('last_skill', 'unknown'),
            'xml_preview': row.get('content_xml_view', '')[:500] + '...' if len(row.get('content_xml_view', '')) > 500 else row.get('content_xml_view', '')
        })
    
    return samples