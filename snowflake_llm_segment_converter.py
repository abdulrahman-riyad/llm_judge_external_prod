"""
Segment Conversion Module for Snowflake LLM Analysis
Converts conversation DataFrames to segment format for LLM processing
Adapted from segment.py to work with DataFrames directly using Snowflake column names
"""

import pandas as pd
import logging
from snowflake_llm_config import get_snowflake_llm_departments_config
from snowflake_llm_helpers import get_execution_id_map

def preprocess_conversation_dataframe_segment(conv_df):
    """
    Preprocess a single conversation DataFrame for segment conversion
    Sort by message timestamp - works directly with Snowflake column names
    """
    # Sort by MESSAGE_SENT_TIME (Snowflake column name)
    if 'MESSAGE_SENT_TIME' in conv_df.columns:
        conv_df = conv_df.sort_values('MESSAGE_SENT_TIME')
    
    return conv_df


def segment_single_conversation(conv_data, department_name):
    """
    Segments a single conversation into parts based on agent or bot changes.
    Adapted from segment.py to use Snowflake column names
    
    Args:
        conv_data: DataFrame containing one conversation's messages
        department_name: Department name for configuration
    
    Returns:
        List of segments: [(agent_name, last_skill, segment_messages), ...]
    """
    segments = []
    current_segment = []
    current_agent = None
    first_agent_or_bot_encountered = False
    last_skill = None
    marking = False
    skill_name_length_limit = 23

    for index, row in conv_data.iterrows():
        # Use Snowflake column names
        sender = str(row["SENT_BY"]).strip().lower()
        message = row["TEXT"]
        skill = row.get("TARGET_SKILL_PER_MESSAGE", "")
        agent_name = row.get("AGENT_NAME", "")  # Assuming this column exists

        # Check marking condition (department-specific logic can be added here)
        if last_skill is None and str(skill).startswith("GPT_DOCTOR"):
            marking = True
        elif marking and (len(str(skill)) > skill_name_length_limit):
            marking = False

        # Identify if it's from agent or bot
        if sender in ["agent", "bot"]:
            if not first_agent_or_bot_encountered:
                current_agent = agent_name if sender == "agent" else "BOT"
                first_agent_or_bot_encountered = True
            else:
                next_agent = agent_name if sender == "agent" else "BOT"
                if next_agent != current_agent:
                    if current_segment:
                        segments.append((current_agent, last_skill, current_segment))
                        current_segment = []
                    current_agent = next_agent

            last_skill = skill  # Always update skill on agent/bot message

        # Add [IDENTIFIER] if marking is True and sender is agent or bot
        if marking and sender in ["agent", "bot"]:
            current_segment.append(f"[IDENTIFIER] {sender.capitalize()}: {message}")
        else:
            current_segment.append(f"{sender.capitalize()}: {message}")

    # Add final segment
    if current_segment:
        segments.append((current_agent, last_skill, current_segment))

    return segments


def convert_single_conversation_to_segment(conv_df, department_name, execution_id):
    """
    Convert a single conversation DataFrame to segment format
    Returns only BOT segments, each as a separate record for individual analysis
    
    Args:
        conv_df: DataFrame containing messages for one conversation
        department_name: Department name for configuration
    
    Returns:
        List of dictionaries with BOT segment data, or empty list if no BOT segments
    """

    # Filter conv_df to only include specific message types
    conv_df = conv_df[conv_df['MESSAGE_TYPE'].str.lower().isin(['tool', 'tool response', 'normal message'])]
    
    if conv_df.empty:
        return None
        
    # Get department configuration
    departments_config = get_snowflake_llm_departments_config()
    if department_name not in departments_config:
        return []
        
    dept_config = departments_config[department_name]
    target_skills = dept_config['bot_skills']
    
    # Preprocess the conversation
    conv_df = preprocess_conversation_dataframe_segment(conv_df.copy())
    
    # Check required columns exist (using Snowflake column names)
    required_columns = ['CONVERSATION_ID', 'MESSAGE_SENT_TIME', 'SENT_BY', 'TEXT']
    missing_columns = [col for col in required_columns if col not in conv_df.columns]
    if missing_columns:
        print(f"    ⚠️  Missing columns for segment conversion: {missing_columns}")
        return None
    
    # Check if conversation contains target skills
    if 'TARGET_SKILL_PER_MESSAGE' in conv_df.columns:
        skills_series = conv_df['TARGET_SKILL_PER_MESSAGE'].fillna('')
        skills = list(set(skills_series.tolist()))
        if not any(skill in target_skills for skill in skills):
            return None
    
    # Check if conversation has bot messages
    has_bot = conv_df['SENT_BY'].str.lower().str.contains('bot', na=False).any()
    if not has_bot:
        return None
    
    # Get conversation ID
    conv_id = conv_df['CONVERSATION_ID'].iloc[0] if len(conv_df) > 0 else "unknown"
    
    # Get customer name if available
    if 'CUSTOMER_NAME' in conv_df.columns and len(conv_df) > 0:
        customer_name = conv_df['CUSTOMER_NAME'].iloc[0]
        if pd.isna(customer_name):
            customer_name = ""
    else:
        customer_name = ""
    
    # Filter for normal messages only
    normal_messages = conv_df[conv_df.get('MESSAGE_TYPE', '').str.upper() == 'NORMAL MESSAGE']
    if normal_messages.empty:
        normal_messages = conv_df  # Fallback to all messages if MESSAGE_TYPE not available
    
    # Segment the conversation
    segments = segment_single_conversation(normal_messages, department_name)
    
    if not segments:
        return []
    
    # Filter to only BOT segments and create separate records
    bot_segments = []
    
    for segment_index, (agent, skill, segment_messages) in enumerate(segments):
        # Only process BOT segments
        if str(agent).upper() == "BOT":
            # Join messages for this BOT segment
            bot_segment_messages = "\n".join(segment_messages)
            
            # Filter: keep only if includes consumer messages
            if "Consumer:" in bot_segment_messages or "consumer:" in bot_segment_messages:
                # Prepare individual BOT segment data
                bot_segment_data = {
                    'conversation_id': str(conv_id),
                    'segment_id': f"{conv_id}_BOT_{segment_index}",  # Unique segment identifier
                    'customer_name': str(customer_name),
                    'last_skill': str(skill) if skill else "",
                    'agent_names': "BOT",  # Only BOT for this segment
                    'messages': bot_segment_messages,
                    'department': department_name,
                    'segment_index': segment_index,
                    'execution_id': execution_id
                }
                bot_segments.append(bot_segment_data)
    
    return bot_segments


def convert_conversations_to_segment_dataframe(filtered_df, department_name):
    """
    Convert filtered DataFrame conversations to segment format without saving files.
    Now returns separate rows for each BOT segment.
    
    Args:
        filtered_df: Filtered DataFrame from Phase 1 processing
        department_name: Department name for configuration
    
    Returns:
        DataFrame with columns: conversation_id, segment_id, customer_name, last_skill, agent_names, messages, department, segment_index
    """
    print(f"    🔄 Converting conversations to BOT segments for {department_name}...")
    
    if filtered_df.empty:
        print(f"    ⚠️  No conversations to convert")
        return pd.DataFrame(columns=['conversation_id', 'segment_id', 'customer_name', 'last_skill', 'agent_names', 'messages', 'department', 'segment_index'])
    
    all_bot_segments = []
    processed_conversations = 0
    total_bot_segments = 0
    
    # Group by conversation ID and process each conversation (using Snowflake column name)
    if 'CONVERSATION_ID' not in filtered_df.columns:
        print(f"    ❌ CONVERSATION_ID column not found in DataFrame")
        return pd.DataFrame(columns=['conversation_id', 'segment_id', 'customer_name', 'last_skill', 'agent_names', 'messages', 'department', 'segment_index'])
    
    execution_id_map = get_execution_id_map(filtered_df, department_name)
    
    for conv_id, conv_df in filtered_df.groupby('CONVERSATION_ID'):
        # Convert conversation to BOT segments (returns list)
        bot_segments = convert_single_conversation_to_segment(conv_df, department_name, execution_id_map.get(conv_id, ''))
        
        if bot_segments:  # If any BOT segments found
            all_bot_segments.extend(bot_segments)  # Add all BOT segments to list
            processed_conversations += 1
            total_bot_segments += len(bot_segments)
    
    print(f"    ✅ Processed {processed_conversations} conversations → {total_bot_segments} BOT segments")
    
    return pd.DataFrame(all_bot_segments)


def validate_segment_conversion(segment_conversations_df, department_name):
    """
    Validate the segment conversion results (now for BOT segments)
    
    Args:
        segment_conversations_df: DataFrame with BOT segment data
        department_name: Department name
    
    Returns:
        Validation results dictionary
    """
    validation_results = {
        'total_bot_segments': len(segment_conversations_df),
        'valid_segment_count': 0,
        'empty_messages_count': 0,
        'unique_conversations': 0,
        'errors': []
    }
    
    if segment_conversations_df.empty:
        validation_results['errors'].append("No BOT segments in DataFrame")
        return validation_results
    
    # Count unique conversations
    if 'conversation_id' in segment_conversations_df.columns:
        validation_results['unique_conversations'] = segment_conversations_df['conversation_id'].nunique()
    
    # Check each BOT segment
    for _, row in segment_conversations_df.iterrows():
        messages = row.get('messages', '')
        
        if not messages or messages.strip() == '':
            validation_results['empty_messages_count'] += 1
        else:
            # Basic validation - should contain both consumer and bot messages
            has_consumer = 'Consumer:' in messages or 'consumer:' in messages
            has_bot = 'Bot:' in messages or 'bot:' in messages
            
            if has_consumer and has_bot:
                validation_results['valid_segment_count'] += 1
            else:
                validation_results['errors'].append(f"Invalid BOT segment structure in {row.get('segment_id', 'unknown')}")
    
    validation_results['success_rate'] = (
        validation_results['valid_segment_count'] / validation_results['total_bot_segments'] * 100
    ) if validation_results['total_bot_segments'] > 0 else 0
    
    return validation_results


def get_segment_sample(segment_conversations_df, sample_size=1):
    """
    Get a sample of BOT segments for inspection
    
    Args:
        segment_conversations_df: DataFrame with BOT segment data
        sample_size: Number of samples to return
    
    Returns:
        Sample BOT segment data
    """
    if segment_conversations_df.empty:
        return []
    
    sample_df = segment_conversations_df.head(sample_size)
    samples = []
    
    for _, row in sample_df.iterrows():
        samples.append({
            'conversation_id': row.get('conversation_id', 'unknown'),
            'segment_id': row.get('segment_id', 'unknown'),
            'segment_index': row.get('segment_index', 'unknown'),
            'department': row.get('department', 'unknown'),
            'last_skill': row.get('last_skill', 'unknown'),
            'agent_names': row.get('agent_names', 'unknown'),
            'messages_preview': row.get('messages', '')[:500] + '...' if len(row.get('messages', '')) > 500 else row.get('messages', '')
        })
    
    return samples