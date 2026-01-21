"""
JSON Conversion Module for Snowflake LLM Analysis
Converts conversation DataFrames to JSON format for LLM processing
Adapted from local JSON converter to work with Snowflake DataFrames directly
"""

import pandas as pd
import json
import re
from datetime import datetime
from snowflake_llm_config import get_snowflake_llm_departments_config
from snowflake_llm_helpers import get_execution_id_map, get_tool_name_and_response


def clean_datetime_format_snowflake(datetime_str):
    """
    Clean datetime format by handling various malformed datetime strings - Snowflake version.
    Converts '7/10/2025 3:50:36â¯PM' to '7/10/2025 3:50:36 PM'
    Converts '7/10/2025 3:50:â¯PM' to '7/10/2025 3:50 PM' 
    """
    if not datetime_str or pd.isna(datetime_str):
        return datetime_str
    
    # Convert to string if not already
    datetime_str = str(datetime_str)
    
    try:
        # Try to parse the datetime as-is
        pd.to_datetime(datetime_str, errors='coerce', format='mixed')
        return datetime_str  # If successful, return as-is
    except:
        # If it fails, apply cleaning
        pass
    
    # Only apply cleaning if the original datetime couldn't be parsed
    # Handle case where we have colon followed by space and AM/PM (missing seconds)
    if ':' in datetime_str and (' PM' in datetime_str or ' AM' in datetime_str):
        # Find the last colon and check if it's followed by space and AM/PM
        last_colon_idx = datetime_str.rfind(':')
        after_colon = datetime_str[last_colon_idx + 1:]
        if after_colon.strip() in ['PM', 'AM']:
            # Remove the colon and everything between it and AM/PM
            cleaned = datetime_str[:last_colon_idx] + ' ' + after_colon.strip()
            return cleaned
    
    # Handle original case with invisible characters
    if len(datetime_str) >= 5:
        # Delete the third, fourth, and fifth to last characters and add space
        # Remove characters at positions -5, -4, -3 (third, fourth, fifth to last)
        cleaned = datetime_str[:-5] + datetime_str[-2:]
        # Add space between time and AM/PM
        cleaned = cleaned[:-2] + ' ' + cleaned[-2:]
        return cleaned
    
    return datetime_str


def preprocess_conversation_dataframe_json(conv_df):
    """
    Preprocess a single conversation DataFrame for JSON conversion
    Sort by message timestamp - works directly with Snowflake column names
    """
    # Sort by MESSAGE_SENT_TIME (Snowflake column name)
    if 'MESSAGE_SENT_TIME' in conv_df.columns:
        conv_df = conv_df.sort_values('MESSAGE_SENT_TIME')
    
    return conv_df


def convert_single_conversation_to_json(conv_df, department_name, include_tool_messages=True, include_private_messages=False, include_tool_output=False):
    """
    Convert a single conversation DataFrame to JSON format - Snowflake version
    Adapted from local convert_conversation_to_json() to work with DataFrames
    
    Args:
        conv_df: DataFrame containing messages for one conversation (using Snowflake column names)
        department_name: Department name for skill filtering
    
    Returns:
        JSON string representation of the conversation
    """
    # Get department configuration
    departments_config = get_snowflake_llm_departments_config()
    if department_name not in departments_config:
        return None
    
    dept_config = departments_config[department_name]
    target_skills = dept_config['bot_skills']  # Only check bot_skills to match XML converter behavior
    
    # Preprocess the conversation
    conv_df = preprocess_conversation_dataframe_json(conv_df)
    
    if conv_df.empty:
        return None
    
    # Filter conv_df to only include specific message types
    if not include_private_messages:
        conv_df = conv_df[conv_df['MESSAGE_TYPE'].str.lower().isin(['tool', 'tool response', 'normal message'])]
    
    if conv_df.empty:
        return None
    
    sent_by_series = conv_df['SENT_BY'].fillna('')
    participants = sorted(list(set(sent_by_series.tolist())))
    # Check if conversation has required participants (bot and consumer)
    has_bot_or_agent = any(p.lower() in ["bot", "agent"] for p in participants)
    has_consumer = any(p.lower() == "consumer" for p in participants)
    if not has_bot_or_agent or not has_consumer:
        return None
    
    # Check if conversation has target skills
    skills = conv_df['TARGET_SKILL_PER_MESSAGE'].unique()
    if not any(skill in target_skills for skill in skills):
        return None
    
    # Get conversation metadata
    conversation_id = str(conv_df['CONVERSATION_ID'].iloc[0])
    
    # Handle customer name safely
    customer_name = "Unknown"
    if 'CUSTOMER_NAME' in conv_df.columns:
        try:
            first_customer = conv_df['CUSTOMER_NAME'].iloc[0]
            if pd.notna(first_customer) and first_customer is not None:
                customer_name = str(first_customer)
        except (IndexError, KeyError):
            pass
    
    # Create conversation object
    conversation = {
        "customer_name": customer_name,
        "chat_id": conversation_id,
        "conversation": []
    }
    
    # Track last message details for duplicate detection
    last_message_time = None
    last_message_text = None
    last_message_sender = None
    last_message_type = None
    
    # Process each message
    for _, row in conv_df.iterrows():
        # Use Snowflake column names
        current_time_raw = row['MESSAGE_SENT_TIME']
        current_text = str(row['TEXT']) if pd.notna(row['TEXT']) else ""
        current_sender = str(row['SENT_BY']) if pd.notna(row['SENT_BY']) else ""
        current_skill = str(row['TARGET_SKILL_PER_MESSAGE']) if pd.notna(row['TARGET_SKILL_PER_MESSAGE']) else ""
        current_type = str(row['MESSAGE_TYPE']).lower() if pd.notna(row['MESSAGE_TYPE']) else ""
        
        # Clean and convert timestamp
        cleaned_time = clean_datetime_format_snowflake(current_time_raw)
        try:
            current_time = pd.to_datetime(cleaned_time, errors='coerce').isoformat()
        except:
            current_time = str(cleaned_time)
        
        # Skip certain message types
        if current_type in ["transfer", "private message", "tool response"] and not include_private_messages:
            continue
        
        # Handle bot messages not from target skills
        if current_sender.lower() == "bot" and current_skill not in target_skills:
            current_sender = "Agent_1"
        
        # Handle empty messages
        if (current_text == "" or pd.isna(row['TEXT'])) and current_type == "normal message":
            current_text = "[Doc/Image]"
        
        # Check for duplicate messages
        is_duplicate = (current_time == last_message_time and 
                       current_text == last_message_text and 
                       current_sender == last_message_sender and 
                       current_type == last_message_type)
        
        # Add tool message if it exists (check for Tools columns)
        if current_type == "tool":
            if current_skill not in target_skills:
                continue
            if include_tool_messages:
                tool_creation_date = row.get('MESSAGE_SENT_TIME', current_time_raw)
                try:
                    tool_timestamp = pd.to_datetime(clean_datetime_format_snowflake(tool_creation_date), errors='coerce').isoformat()
                except:
                    tool_timestamp = str(tool_creation_date)

                tool_name, tool_output, tool_args = get_tool_name_and_response(conv_df, current_text)
                
                if include_tool_output:
                    tool_message = {
                        "timestamp": tool_timestamp,
                        "sender": "Bot",
                        "type": "tool",
                        "tool": tool_name,
                        "args": tool_args or "{}",
                        "result": tool_output or "{}"
                    }
                else:
                    tool_message = {
                        "timestamp": tool_timestamp,
                        "sender": "Bot",
                        "type": "tool",
                        "tool": tool_name,
                    }
                conversation["conversation"].append(tool_message)
            continue
        
        # Add regular message if not duplicate and has content
        if current_text and not is_duplicate:
            message = {
                "timestamp": current_time,
                "sender": current_sender,
                "type": current_type,
                "content": current_text
            }
            conversation["conversation"].append(message)
            
            # Update last message details
            last_message_time = current_time
            last_message_text = current_text
            last_message_sender = current_sender
            last_message_type = current_type
    
    # Return JSON string
    try:
        return json.dumps(conversation, indent=2, ensure_ascii=False)
    except Exception as e:
        # Return a simplified version if JSON serialization fails
        simplified_conversation = {
            "customer_name": customer_name,
            "chat_id": conversation_id,
            "conversation": f"JSON_SERIALIZATION_ERROR: {str(e)}"
        }
        return json.dumps(simplified_conversation, indent=2, ensure_ascii=False)


def convert_conversations_to_json_dataframe(filtered_df, department_name, include_tool_messages=True, include_private_messages=False, include_tool_output=False):
    """
    Convert filtered conversations DataFrame to JSON format for LLM analysis
    Following the same pattern as convert_conversations_to_xml_dataframe()
    
    Args:
        filtered_df: Filtered DataFrame from Phase 1 (using Snowflake column names)
        department_name: Department name for configuration
    
    Returns:
        DataFrame with conversation JSON data ready for LLM processing
    """
    print(f"    🔄 Converting conversations to JSON format for {department_name}...")
    
    if filtered_df.empty:
        print(f"    ⚠️  No filtered data for JSON conversion")
        return pd.DataFrame()
    
    # Group by conversation ID
    conversations = filtered_df.groupby('CONVERSATION_ID')
    
    json_data = []
    successful_conversions = 0
    failed_conversions = 0

    execution_id_map = get_execution_id_map(filtered_df, department_name)
    
    for conv_id, conv_df in conversations:
        # Convert single conversation to JSON
        json_content = convert_single_conversation_to_json(conv_df, department_name, include_tool_messages, include_private_messages, include_tool_output)
        
        if json_content:
            # Extract metadata for the result DataFrame - with safe handling for optional columns
            customer_name = "Unknown"
            if 'CUSTOMER_NAME' in conv_df.columns:
                try:
                    first_customer = conv_df['CUSTOMER_NAME'].iloc[0]
                    if pd.notna(first_customer) and first_customer is not None:
                        customer_name = str(first_customer)
                except (IndexError, KeyError):
                    pass
            
            # Handle agent names safely - column might not exist or contain None values
            agent_names = ""
            if 'AGENT_NAME' in conv_df.columns:
                try:
                    agent_rows = conv_df[conv_df['SENT_BY'].str.upper() == 'AGENT']
                    if not agent_rows.empty:
                        agent_names_list = agent_rows['AGENT_NAME'].unique()
                        # Filter out None/NaN values and convert to strings
                        agent_names_list = [str(name) for name in agent_names_list 
                                          if pd.notna(name) and name is not None and str(name).strip() != 'nan']
                        agent_names = ", ".join(agent_names_list)
                except (KeyError, AttributeError):
                    pass
            
            # Handle last skill safely
            last_skill = conv_df['SKILL'].iloc[0] if 'SKILL' in conv_df.columns else ""
            
            # Extract ID columns for user type determination
            client_id = conv_df['CLIENT_ID'].iloc[0] if 'CLIENT_ID' in conv_df.columns and pd.notna(conv_df['CLIENT_ID'].iloc[0]) else None
            maid_id = conv_df['MAID_ID'].iloc[0] if 'MAID_ID' in conv_df.columns and pd.notna(conv_df['MAID_ID'].iloc[0]) else None
            applicant_id = conv_df['APPLICANT_ID'].iloc[0] if 'APPLICANT_ID' in conv_df.columns and pd.notna(conv_df['APPLICANT_ID'].iloc[0]) else None
            
            # Determine USER_TYPE: 'maid' if maid_id or applicant_id exists, otherwise 'client'
            user_type = 'maid' if (maid_id is not None or applicant_id is not None) else 'client'
            
            json_record = {
                'conversation_id': conv_id,
                'customer_name': customer_name,
                'agent_names': agent_names,
                'last_skill': last_skill,
                'content_json_view': json_content,  # The JSON string for LLM processing
                'message_count': len(conv_df),
                'conversion_status': 'SUCCESS',
                'execution_id': execution_id_map.get(conv_id, ''),
                'shadowed_by': conv_df['SHADOWED_BY'].iloc[0],
                'user_type': user_type,
            }
            
            json_data.append(json_record)
            successful_conversions += 1
        else:
            failed_conversions += 1
            # Don't add skipped conversations to raw data table
            # They will be tracked in the failed_conversions count only
            print(f"    ⏭️  Skipped conversation {conv_id} (not added to raw data table)")
    
    if json_data:
        result_df = pd.DataFrame(json_data)
        print(f"    ✅ JSON conversion completed: {successful_conversions} successful, {failed_conversions} failed")
        return result_df
    else:
        print(f"    ❌ No conversations could be converted to JSON")
        return pd.DataFrame()


def validate_json_conversion(json_df, department_name):
    """
    Validate JSON conversion results
    Following the same pattern as validate_xml_conversion()
    
    Args:
        json_df: DataFrame with JSON conversion results
        department_name: Department name
    
    Returns:
        Dictionary with validation statistics
    """
    if json_df.empty:
        return {
            'total_conversations': 0,
            'valid_json_count': 0,
            'failed_json_count': 0,
            'success_rate': 0,
            'average_json_length': 0,
            'validation_status': 'NO_DATA'
        }
    
    total_conversations = len(json_df)
    valid_json_count = len(json_df[json_df['conversion_status'] == 'SUCCESS'])
    failed_json_count = len(json_df[json_df['conversion_status'] == 'FAILED'])
    
    # Calculate average JSON length for successful conversions
    successful_jsons = json_df[json_df['conversion_status'] == 'SUCCESS']['content_json_view']
    average_json_length = successful_jsons.str.len().mean() if not successful_jsons.empty else 0
    
    success_rate = (valid_json_count / total_conversations * 100) if total_conversations > 0 else 0
    
    validation_results = {
        'total_conversations': total_conversations,
        'valid_json_count': valid_json_count,
        'failed_json_count': failed_json_count,
        'success_rate': success_rate,
        'average_json_length': int(average_json_length) if average_json_length > 0 else 0,
        'validation_status': 'SUCCESS' if success_rate > 80 else 'WARNING' if success_rate > 50 else 'FAILED'
    }
    
    return validation_results