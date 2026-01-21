"""
Message-Level Conversion Module for Tool Evaluation

This module converts conversation-level data into message-level segments,
specifically for tool evaluation where each consumer message is analyzed independently.

ENVIRONMENT AGNOSTIC:
This module uses only pandas and standard Python - no Snowpark dependencies.
It works in all environments:
- Inside Snowflake (via Snowpark)
- Google Colab
- GitHub Actions
- Local development

Input: pandas DataFrame with conversation messages
Output: pandas DataFrame with message-level segments
"""

import pandas as pd
import json
import re
from typing import Optional, List
from snowflake_llm_helpers import get_tool_name_and_response
from snowflake_llm_config import get_snowflake_llm_departments_config


def _is_new_conversation_case(all_messages: pd.DataFrame, messages_in_middle_df: pd.DataFrame) -> bool:
    """
    Check if the messages indicate a new conversation case.
    
    Args:
        messages_in_middle_df: DataFrame with bot/system messages between current consumer and previous consumer
        
    Returns:
        True if top 2 bot messages have new conversation skills (PROSPECT_NATIONALITY_SERVICE_N8N or GPT_UNKNOWN_IDENTIFIER_N8N)
    """

    second_messages_in_middle_df = _get_messages_in_middle(all_messages, messages_in_middle_df['MESSAGE_SENT_TIME'].min())
    messages_in_middle_df = pd.concat([messages_in_middle_df, second_messages_in_middle_df])
    
    if messages_in_middle_df.empty:
        return False
            
    # Check if any of the bot or system messages have new conversation skills
    new_conversation_skills = ['PROSPECT_NATIONALITY_SERVICE_N8N', 'GPT_UNKNOWN_IDENTIFIER_N8N', 'PROSPECT_NATIONALITY_SERVICE', 'GPT_UNKNOWN_IDENTIFIER']
    for _, bot_msg in messages_in_middle_df.iterrows():
        bot_skill = bot_msg.get('TARGET_SKILL_PER_MESSAGE')
        if not pd.isna(bot_skill):
            skill_upper = str(bot_skill).upper()
            if skill_upper in [s.upper() for s in new_conversation_skills]:
                return True
    
    return False


def _is_transfer_case(messages_in_middle_df: pd.DataFrame, bot_skills: List[str]) -> bool:
    """
    Check if the messages indicate a transfer case.
    
    Args:
        messages_in_middle_df: DataFrame with bot/system messages between current consumer and previous consumer
        bot_skills: List of bot skills for the department
        
    Returns:
        True if any message in messages_in_middle_df has a skill different from bot_skills
    """
    if messages_in_middle_df.empty:
        return False
    
    bot_skills_upper = [s.upper() for s in bot_skills]
    
    
    # Check if any message has a skill not in bot_skills
    for _, msg in messages_in_middle_df.iterrows():
        msg_skill = msg.get('TARGET_SKILL_PER_MESSAGE')
        if not pd.isna(msg_skill):
            msg_skill_upper = str(msg_skill).upper()
            if msg_skill_upper not in bot_skills_upper:
                return True
    
    return False


def _get_messages_in_middle(all_messages: pd.DataFrame, current_msg_time: float) -> pd.DataFrame:
    """
    Get messages in middle of conversation.
    
    Args:
        all_messages: DataFrame with ALL messages (unfiltered) from the conversation
        current_msg_time: MESSAGE_SENT_TIME of the current consumer message
        
    Returns:
        DataFrame with messages in middle of conversation
    """
    previous_messages = all_messages[
        all_messages['MESSAGE_SENT_TIME'] < current_msg_time
    ].copy()
    
    if previous_messages.empty:
        return pd.DataFrame()
    
    # Sort by time (most recent first for easier processing)
    previous_messages = previous_messages.sort_values('MESSAGE_SENT_TIME', ascending=False)
    
    # Step 1: Create messages_in_middle_df
    # Contains bot/system messages between current_msg_time and first consumer message encountered
    messages_in_middle_df = pd.DataFrame()
    found_first_non_consumer = False
    
    for idx, msg in previous_messages.iterrows():
        sent_by = msg.get('SENT_BY', '').lower()
        msg_type = msg.get('MESSAGE_TYPE', '').lower()
        
        # Stop when we encounter a consumer message (this is the boundary)
        if found_first_non_consumer and sent_by == 'consumer':
            break
        
        if (not found_first_non_consumer and sent_by != 'consumer'):
            found_first_non_consumer = True

        # Collect bot/system messages
        if (sent_by == 'bot' or sent_by == 'system'):
            messages_in_middle_df = pd.concat([messages_in_middle_df, msg.to_frame().T], ignore_index=True)
    
    # Sort messages_in_middle_df descending by MESSAGE_SENT_TIME
    if not messages_in_middle_df.empty:
        messages_in_middle_df = messages_in_middle_df.sort_values('MESSAGE_SENT_TIME', ascending=False)

    return messages_in_middle_df

    

def _detect_transfer_or_new_conversation(all_messages: pd.DataFrame, current_msg_time, current_consumer_skill: str, bot_skills: List[str]) -> tuple:
    """
    Detect if there's a transfer or new conversation before the current consumer message.
    
    Args:
        all_messages: DataFrame with ALL messages (unfiltered) from the conversation
        current_msg_time: MESSAGE_SENT_TIME of the current consumer message
        current_consumer_skill: TARGET_SKILL_PER_MESSAGE of the current consumer message
        bot_skills: List of bot skills for the department
        
    Returns:
        Tuple (is_new_conversation, is_transfer, accumulated_messages)
        - is_new_conversation: bool
        - is_transfer: bool  
        - accumulated_messages: List of consumer message texts to concatenate
    """
    # Get messages before current consumer message
    messages_in_middle_df = _get_messages_in_middle(all_messages, current_msg_time)
    
    # Step 2: Check for New Conversation Case
    if not messages_in_middle_df.empty and _is_new_conversation_case(all_messages, messages_in_middle_df):
        # Get all consumer messages sent before the least MESSAGE_SENT_TIME in messages_in_middle_df
        min_time_in_middle = messages_in_middle_df['MESSAGE_SENT_TIME'].min()
        
        all_consumer_messages = all_messages[
            (all_messages['SENT_BY'].str.lower() == 'consumer') &
            (all_messages['MESSAGE_SENT_TIME'] < min_time_in_middle)
        ].sort_values('MESSAGE_SENT_TIME')
        
        accumulated_texts = []
        for _, consumer_msg in all_consumer_messages.iterrows():
            msg_text = consumer_msg.get('TEXT', '')
            if msg_text and str(msg_text).strip():
                accumulated_texts.append(str(msg_text).strip())
        
        return (True, False, accumulated_texts)
    
    # Step 3: Check for Transfer Case
    if not messages_in_middle_df.empty and _is_transfer_case(messages_in_middle_df, bot_skills):
        # Get consumer messages between least MESSAGE_SENT_TIME in messages_in_middle_df 
        # and the time of any non-consumer message before that (looking backwards)
        min_time_in_middle = messages_in_middle_df['MESSAGE_SENT_TIME'].min()
        
        # Find the first non-consumer message before min_time_in_middle (looking backwards)
        messages_before_min = all_messages[
            all_messages['MESSAGE_SENT_TIME'] < min_time_in_middle
        ].sort_values('MESSAGE_SENT_TIME', ascending=False)  # Most recent first
        
        max_time_after_non_consumer = min_time_in_middle  # Default to min_time_in_middle if no non-consumer found before
        for idx, msg in messages_before_min.iterrows():
            sent_by = msg.get('SENT_BY', '').lower()
            msg_type = msg.get('MESSAGE_TYPE', '').lower()
            
            # If we hit a non-consumer message (bot/system), use its time as the lower bound
            if (sent_by == 'bot' or sent_by == 'system'):
                max_time_after_non_consumer = msg['MESSAGE_SENT_TIME']
                break
        
        # Get consumer messages between max_time_after_non_consumer and min_time_in_middle
        consumer_messages_in_range = all_messages[
            (all_messages['SENT_BY'].str.lower() == 'consumer') &
            (all_messages['MESSAGE_SENT_TIME'] > max_time_after_non_consumer) &
            (all_messages['MESSAGE_SENT_TIME'] < min_time_in_middle)
        ].sort_values('MESSAGE_SENT_TIME')
        
        accumulated_texts = []
        current_consumer_skill_upper = str(current_consumer_skill).upper() if not pd.isna(current_consumer_skill) else ''
        bot_skills_upper = [s.upper() for s in bot_skills]
        
        for _, consumer_msg in consumer_messages_in_range.iterrows():
            msg_text = consumer_msg.get('TEXT', '')
            if msg_text and str(msg_text).strip():
                accumulated_texts.append(str(msg_text).strip())
        
        if accumulated_texts:
            return (False, True, accumulated_texts)
    
    return (False, False, [])


def convert_conversations_to_message_segments(conversations_df: pd.DataFrame, department_name: str, debug: bool = False) -> pd.DataFrame:
    """
    Convert filtered conversations to message-level segments for tool evaluation.
    
    Process:
    1. Filter messages by skill (matching department skill)
    2. Sort by message time
    3. Identify consumer messages
    4. Handle consecutive consumer messages (keep only last one)
    5. Detect ACTUAL_TOOLS_CALLED from subsequent tool messages
    6. Build message segment records
    
    Args:
        conversations_df: DataFrame with conversation data from CHAT_CONVERSATIONS_VIEW
        department_name: Department name (e.g., 'MV_Resolvers')
    
    Returns:
        DataFrame ready for TOOL_EVAL_RAW_DATA insertion with columns:
        - CONVERSATION_ID
        - MESSAGE_ID
        - MESSAGE (consumer message text)
        - EXECUTION_ID
        - ACTUAL_TOOLS_CALLED (JSON array as string)
        - TARGET_SKILL
        - CUSTOMER_NAME
        - AGENT_NAMES
        - SHADOWED_BY
        - USER_TYPE
    """
    print(f"🔄 Converting to message segments for tool evaluation...")
    print(f"   Input: {len(conversations_df)} rows")
    
    if conversations_df.empty:
        print(f"   ⚠️  Empty input DataFrame")
        return pd.DataFrame()
    
    # Get department bot skills from config
    departments_config = get_snowflake_llm_departments_config()
    if department_name not in departments_config:
        return None
    
    dept_config = departments_config[department_name]
    bot_skills = dept_config['bot_skills']  # Only check bot_skills to match XML converter behavior
    
    print(f"   🎯 Department bot skills: {bot_skills}")
    
    # If no bot_skills in config, fall back to department name
    if not bot_skills:
        bot_skills = [department_name]
    
    # Group by conversation
    grouped = conversations_df.groupby('CONVERSATION_ID')
    
    message_segments = []
    total_conversations = len(grouped)
    processed_conversations = 0
    skipped_conversations = 0
    skipped_no_execution_id = 0
    skipped_agent_takeover = 0
    
    for conv_id, conv_messages in grouped:
        # Sort messages chronologically
        conv_messages = conv_messages.sort_values('MESSAGE_SENT_TIME').copy()
        
        # IMPORTANT: Save a copy of ALL messages (unfiltered) for transfer/new conversation detection
        # We need to see messages with different skills to detect transfers and new conversations
        all_messages_unfiltered = conv_messages.copy()
        
        # Step 1: Filter messages by skill (match any bot skill)
        # Also include consumer messages with null skill if next message is from bot_skills
        
        # Add next message's skill as a helper column
        conv_messages['NEXT_SKILL'] = conv_messages['TARGET_SKILL_PER_MESSAGE'].shift(-1)
        
        # Build filter conditions
        skill_matches = conv_messages['TARGET_SKILL_PER_MESSAGE'].str.upper().isin([skill.upper() for skill in bot_skills])
        is_consumer = conv_messages['SENT_BY'].str.lower() == 'consumer'
        skill_is_null = conv_messages['TARGET_SKILL_PER_MESSAGE'].isna()
        next_skill_matches = conv_messages['NEXT_SKILL'].str.upper().isin([skill.upper() for skill in bot_skills])
        
        # Keep messages that either:
        # 1. Have a skill matching bot_skills
        # 2. OR are consumer messages with null skill AND next message has bot_skills
        conv_messages = conv_messages[
            skill_matches | (is_consumer & skill_is_null & next_skill_matches)
        ].copy()
        
        # Drop the helper column
        conv_messages = conv_messages.drop('NEXT_SKILL', axis=1, errors='ignore')
        
        if conv_messages.empty:
            skipped_conversations += 1
            continue
        
        # Step 2 & 3: Find valid consumer messages (remove consecutive duplicates)
        # Logic: Keep only the LAST consumer message before each bot response
        valid_messages = []
        current_consumer_group = []
        
        for idx, msg in conv_messages.iterrows():
            sent_by = msg['SENT_BY'].lower()
            message_type = msg['MESSAGE_TYPE'].lower()

            if message_type != 'normal message':
                continue
            
            if sent_by == 'consumer':
                # Add to current consumer group
                current_consumer_group.append(msg)
            else:
                # Bot message encountered - save the LAST consumer message from the group
                if current_consumer_group:
                    valid_messages.append(current_consumer_group[-1])  # Keep LAST only
                    current_consumer_group = []
        
        # Handle any remaining consumer messages at the end of conversation
        if current_consumer_group:
            valid_messages.append(current_consumer_group[-1])
        
        if not valid_messages:
            skipped_conversations += 1
            continue
        
        # Step 4: For each valid consumer message, extract data
        for msg in valid_messages:
            message_id = msg['MESSAGE_ID']
            message_text = msg['TEXT']
            target_skill = msg['TARGET_SKILL_PER_MESSAGE']
            customer_name = conv_messages.iloc[0]['CUSTOMER_NAME'] if 'CUSTOMER_NAME' in conv_messages.columns else ''
            
            # Step 5: Find execution_id and ACTUAL_TOOLS_CALLED
            # Look at messages between this consumer message and the next consumer message
            msg_time = msg['MESSAGE_SENT_TIME']
            next_messages = conv_messages[
                (conv_messages['MESSAGE_SENT_TIME'] > msg_time)
            ].copy()
            
            # Extract execution_id from first message that has it (usually the bot response)
            # Step 1: Find execution_id from next messages
            execution_id = None
            for _, next_msg in next_messages.iterrows():
                # First, try to get execution_id from current message
                next_exec_id = next_msg.get('EXECUTION_ID')
                
                # Check if it's a valid execution_id (not None, not NaN, not empty)
                if next_exec_id is not None and not pd.isna(next_exec_id):
                    # Convert float to int if needed (e.g., 586708.0 -> 586708)
                    if isinstance(next_exec_id, float):
                        execution_id = str(int(next_exec_id))
                    else:
                        execution_id = str(next_exec_id).strip()
                    
                    # Final validation - make sure it's not empty
                    if execution_id:
                        break  # Found valid execution_id, stop searching
                    else:
                        execution_id = None
                
                # If we didn't find execution_id yet, check if we should stop searching
                # Stop if we hit another consumer message or bot normal message (without execution_id)
                if next_msg['SENT_BY'].lower() == 'consumer' or (next_msg['SENT_BY'].lower() == 'bot' and next_msg['TARGET_SKILL_PER_MESSAGE'].upper() in bot_skills and next_msg['MESSAGE_TYPE'].lower() == 'normal message'):
                    break
            
            # Skip if no execution_id found
            if not execution_id or execution_id == '' or pd.isna(execution_id):
                skipped_no_execution_id += 1
                continue
            
            # Step 2: Collect all tools from messages with same execution_id
            actual_tools = []
            has_transfer_tool = False
            
            # Filter messages with same execution_id and tool message type
            # Use conv_messages (entire conversation) instead of next_messages since execution_id is unique
            # This handles all edge cases including consecutive consumer messages and race conditions
            tool_messages = conv_messages[
                (conv_messages['EXECUTION_ID'].notna()) &
                (conv_messages['EXECUTION_ID'].apply(lambda x: str(int(x)) if isinstance(x, float) else str(x)).str.strip() == execution_id) &
                (conv_messages['MESSAGE_TYPE'].str.lower().str.contains('tool', na=False))
            ]
            
            for _, tool_msg in tool_messages.iterrows():
                # Extract tool name using helper function
                tool_name, tool_output, tool_args = get_tool_name_and_response(conv_messages, tool_msg['TEXT'])
                if tool_name and tool_name not in actual_tools:
                    actual_tools.append(tool_name)
                    # Check if this is a transfer tool
                    if 'transfer' in tool_name.lower():
                        has_transfer_tool = True
            
            # Step 3: Check for agent takeover (first non-tool message)
            agent_takeover_detected = False
            for _, next_msg in next_messages.iterrows():
                # Stop if we hit another consumer message
                if next_msg['SENT_BY'].lower() == 'consumer' or (next_msg['SENT_BY'].lower() == 'bot' and next_msg['TARGET_SKILL_PER_MESSAGE'].upper() in bot_skills and next_msg['MESSAGE_TYPE'].lower() == 'normal message'):
                    break
                
                # Check for first non-tool normal message
                msg_type = next_msg.get('MESSAGE_TYPE', '').lower()
                if msg_type == 'normal message':
                    # Check if agent took over without transfer tool
                    if next_msg['SENT_BY'].lower() == 'agent' and not has_transfer_tool:
                        # Optional: Check skill difference (commented out for now)
                        # agent_skill = next_msg.get('TARGET_SKILL_PER_MESSAGE', '')
                        # skill_differs = agent_skill != target_skill
                        # if skill_differs:
                        agent_takeover_detected = True
                    break  # Found first normal message, stop checking
            
            # Skip this consumer message if agent takeover was detected
            if agent_takeover_detected:
                skipped_agent_takeover += 1
                print(f"   🔄 Agent takeover detected for message {message_id} CONVERSATION_ID: {conv_id}")
                continue
            
            # Step 6: Detect Transfer or New Conversation and accumulate context messages
            # Use unfiltered messages to see all skills (including transfers and new conversations)
            # is_new_conv, is_transfer, accumulated_messages = _detect_transfer_or_new_conversation(
            #     all_messages_unfiltered, msg_time, target_skill, bot_skills
            # )
            
            
            # Build final message text: accumulated messages + current message
            final_message_text = message_text
            
            if False:
                print(f"   🔄 Detected Transfer? {is_transfer} || New Conversation? {is_new_conv} for message {message_id} CONVERSATION_ID: {conv_id}")
                # Concatenate accumulated messages with current message (comma-separated)
                accumulated_text = ', '.join(accumulated_messages)
                final_message_text = f"{accumulated_text}, {message_text}"
            
            # Step 7: Build message segment record
            segment = {
                'CONVERSATION_ID': conv_id,
                'MESSAGE_ID': message_id,
                'MESSAGE': final_message_text,  # Use concatenated message if transfer/new conversation detected
                'EXECUTION_ID': execution_id,
                'ACTUAL_TOOLS_CALLED': json.dumps(actual_tools),  # JSON array as string
                'TARGET_SKILL': target_skill,
                'CUSTOMER_NAME': customer_name,
                'AGENT_NAMES': '',  # Not applicable for message-level
                'SHADOWED_BY': msg.get('SHADOWED_BY', ''),
                'USER_TYPE': 'client'  # Default to client for now
            }
            
            message_segments.append(segment)
        
        processed_conversations += 1
    
    # Convert to DataFrame
    result_df = pd.DataFrame(message_segments)
    
    print(f"   ✅ Converted to {len(result_df)} message segments")
    print(f"   📊 From {processed_conversations}/{total_conversations} conversations")
    print(f"   ⏭️  Skipped {skipped_conversations} conversations (no valid consumer messages)")
    if skipped_no_execution_id > 0:
        print(f"   ⚠️  Skipped {skipped_no_execution_id} message segments (no execution_id)")
    if skipped_agent_takeover > 0:
        print(f"   🚫 Skipped {skipped_agent_takeover} message segments (implicit agent takeover without transfer)")
    
    return result_df


def convert_conversations_to_message_segments_mv_resolvers(
    conversations_df: pd.DataFrame, 
    department_name: str
) -> pd.DataFrame:
    """
    Convert conversations to message segments for MV_Resolvers tool evaluation.
    
    SIMPLIFIED VERSION (no transfer/new conversation logic):
    1. Filter messages by skill
    2. Find valid consumer messages (keep last in consecutive groups)
    3. Extract ACTUAL_TOOLS_CALLED
    4. Extract EXECUTION_ID
    5. Extract ChatbotResponseToConsumerMessage (first bot normal message)
    6. Build MESSAGE as JSON with 4 fields (HistoryOfChatbot empty initially)
    
    Args:
        conversations_df: DataFrame with conversation data
        department_name: Department name (e.g., 'MV_Resolvers')
    
    Returns:
        DataFrame with columns:
        - CONVERSATION_ID
        - MESSAGE_ID
        - MESSAGE (stringified JSON with 4 fields)
        - EXECUTION_ID
        - ACTUAL_TOOLS_CALLED (JSON array as string)
        - TARGET_SKILL
        - CUSTOMER_NAME
        - AGENT_NAMES
        - SHADOWED_BY
        - USER_TYPE
    """
    print(f"🔄 Converting to message segments for MV_Resolvers tool evaluation...")
    print(f"   Input: {len(conversations_df)} rows")
    
    if conversations_df.empty:
        print(f"   ⚠️  Empty input DataFrame")
        return pd.DataFrame()
    
    # Get department bot skills from config
    departments_config = get_snowflake_llm_departments_config()
    if department_name not in departments_config:
        print(f"   ❌ Department {department_name} not in config")
        return pd.DataFrame()
    
    dept_config = departments_config[department_name]
    bot_skills = dept_config['bot_skills']
    
    print(f"   🎯 Department bot skills: {bot_skills}")
    
    if not bot_skills:
        bot_skills = [department_name]
    
    # Group by conversation
    grouped = conversations_df.groupby('CONVERSATION_ID')
    
    message_segments = []
    total_conversations = len(grouped)
    processed_conversations = 0
    skipped_conversations = 0
    skipped_no_execution_id = 0
    
    for conv_id, conv_messages in grouped:
        # Sort messages chronologically
        conv_messages = conv_messages.sort_values('MESSAGE_SENT_TIME').copy()
        
        # Step 1: Filter messages by skill
        conv_messages['NEXT_SKILL'] = conv_messages['TARGET_SKILL_PER_MESSAGE'].shift(-1)
        
        skill_matches = conv_messages['TARGET_SKILL_PER_MESSAGE'].str.upper().isin([skill.upper() for skill in bot_skills])
        is_consumer = conv_messages['SENT_BY'].str.lower() == 'consumer'
        skill_is_null = conv_messages['TARGET_SKILL_PER_MESSAGE'].isna()
        next_skill_matches = conv_messages['NEXT_SKILL'].str.upper().isin([skill.upper() for skill in bot_skills])
        
        conv_messages = conv_messages[
            skill_matches | (is_consumer & skill_is_null & next_skill_matches)
        ].copy()
        
        conv_messages = conv_messages.drop('NEXT_SKILL', axis=1, errors='ignore')
        
        if conv_messages.empty:
            skipped_conversations += 1
            continue
        
        # Step 2: Find valid consumer messages (keep last in consecutive groups)
        valid_messages = []
        current_consumer_group = []
        
        for idx, msg in conv_messages.iterrows():
            sent_by = msg['SENT_BY'].lower()
            message_type = msg['MESSAGE_TYPE'].lower()
            
            if message_type != 'normal message':
                continue
            
            if sent_by == 'consumer':
                current_consumer_group.append(msg)
            else:
                # Bot message encountered - save LAST consumer message
                if current_consumer_group:
                    valid_messages.append(current_consumer_group[-1])
                    current_consumer_group = []
        
        # Handle remaining consumer messages at end
        if current_consumer_group:
            valid_messages.append(current_consumer_group[-1])
        
        if not valid_messages:
            skipped_conversations += 1
            continue
        
        # Step 3-6: Extract data for each valid consumer message
        for msg in valid_messages:
            message_id = msg['MESSAGE_ID']
            message_text = msg['TEXT']
            target_skill = msg['TARGET_SKILL_PER_MESSAGE']
            customer_name = conv_messages.iloc[0]['CUSTOMER_NAME'] if 'CUSTOMER_NAME' in conv_messages.columns else ''
            
            # Find messages after this consumer message
            msg_time = msg['MESSAGE_SENT_TIME']
            next_messages = conv_messages[
                (conv_messages['MESSAGE_SENT_TIME'] > msg_time)
            ].copy()
            
            # Step 1: Find execution_id from next messages
            execution_id = None
            for _, next_msg in next_messages.iterrows():
                # First, try to get execution_id from current message
                next_exec_id = next_msg.get('EXECUTION_ID')
                
                # Check if it's a valid execution_id (not None, not NaN, not empty)
                if next_exec_id is not None and not pd.isna(next_exec_id):
                    # Convert float to int if needed (e.g., 586708.0 -> 586708)
                    if isinstance(next_exec_id, float):
                        execution_id = str(int(next_exec_id))
                    else:
                        execution_id = str(next_exec_id).strip()
                    
                    # Final validation - make sure it's not empty
                    if execution_id:
                        break  # Found valid execution_id, stop searching
                    else:
                        execution_id = None
                
                # If we didn't find execution_id yet, check if we should stop searching
                # Stop if we hit another consumer message
                if next_msg['SENT_BY'].lower() == 'consumer':
                    break
            
            # Skip if no execution_id found
            if not execution_id or execution_id == '' or pd.isna(execution_id):
                skipped_no_execution_id += 1
                continue
            
            # Step 2: Collect all tools from messages with same execution_id
            actual_tools = []
            
            # Filter messages with same execution_id and tool message type
            tool_messages = next_messages[
                (next_messages['EXECUTION_ID'].notna()) &
                (next_messages['EXECUTION_ID'].apply(lambda x: str(int(x)) if isinstance(x, float) else str(x)).str.strip() == execution_id) &
                (next_messages['MESSAGE_TYPE'].str.lower().str.contains('tool', na=False))
            ]
            
            for _, tool_msg in tool_messages.iterrows():
                # Extract tool name using helper function
                tool_name, tool_output, tool_args = get_tool_name_and_response(conv_messages, tool_msg['TEXT'])
                if tool_name and tool_name not in actual_tools:
                    actual_tools.append(tool_name)
            
            # Skip if no tools called
            if not actual_tools:
                skipped_no_execution_id += 1
                continue
            
            # Step 3: Extract ChatbotResponseToConsumerMessage
            chatbot_response = ""
            for _, next_msg in next_messages.iterrows():
                # Stop if we hit another consumer message
                if next_msg['SENT_BY'].lower() == 'consumer':
                    break
                
                # Extract ChatbotResponseToConsumerMessage (first bot normal message with bot_skills)
                if (not chatbot_response and 
                    next_msg['SENT_BY'].lower() == 'bot' and 
                    next_msg['MESSAGE_TYPE'].lower() == 'normal message' and
                    str(next_msg['TARGET_SKILL_PER_MESSAGE']).upper() in [s.upper() for s in bot_skills]):
                    chatbot_response = str(next_msg.get('TEXT', '')).strip()
                    break  # Found chatbot response, stop searching
            
            # Build MESSAGE as JSON with 4 fields
            message_json = {
                "HistoryOfChatbot": "",  # Empty - will be populated in processor
                "ConsumerLastMessage": message_text,
                "ToolCalled": actual_tools,
                "ChatbotResponseToConsumerMessage": chatbot_response
            }
            
            # Build segment record
            segment = {
                'CONVERSATION_ID': conv_id,
                'MESSAGE_ID': message_id,
                'MESSAGE': json.dumps(message_json),  # Stringified JSON
                'EXECUTION_ID': execution_id,
                'ACTUAL_TOOLS_CALLED': json.dumps(actual_tools),
                'TARGET_SKILL': target_skill,
                'CUSTOMER_NAME': customer_name,
                'AGENT_NAMES': '',
                'SHADOWED_BY': msg.get('SHADOWED_BY', ''),
                'USER_TYPE': 'client'
            }
            
            message_segments.append(segment)
        
        processed_conversations += 1
    
    result_df = pd.DataFrame(message_segments)
    
    print(f"   ✅ Converted to {len(result_df)} message segments")
    print(f"   📊 From {processed_conversations}/{total_conversations} conversations")
    print(f"   ⏭️  Skipped {skipped_conversations} conversations (no valid consumer messages)")
    if skipped_no_execution_id > 0:
        print(f"   ⚠️  Skipped {skipped_no_execution_id} message segments (no execution_id)")
    
    return result_df


def validate_message_segment_conversion(segments_df: pd.DataFrame) -> dict:
    """
    Validate the message segments DataFrame.
    
    Args:
        segments_df: DataFrame with message segments
    
    Returns:
        Validation results dictionary with:
        - is_valid: bool
        - errors: list of error messages
        - warnings: list of warning messages
        - stats: dictionary of statistics
    """
    errors = []
    warnings = []
    stats = {}
    
    # Check if DataFrame is empty
    if segments_df.empty:
        errors.append("DataFrame is empty")
        return {
            'is_valid': False,
            'errors': errors,
            'warnings': warnings,
            'stats': stats
        }
    
    # Required columns
    required_columns = [
        'CONVERSATION_ID', 'MESSAGE_ID', 'MESSAGE', 'EXECUTION_ID',
        'ACTUAL_TOOLS_CALLED', 'TARGET_SKILL', 'CUSTOMER_NAME',
        'AGENT_NAMES', 'SHADOWED_BY', 'USER_TYPE'
    ]
    
    # Check for missing columns
    missing_cols = [col for col in required_columns if col not in segments_df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")
    
    # Check for null values in critical columns
    critical_columns = ['CONVERSATION_ID', 'MESSAGE_ID', 'MESSAGE']
    for col in critical_columns:
        if col in segments_df.columns:
            null_count = segments_df[col].isnull().sum()
            if null_count > 0:
                errors.append(f"Column {col} has {null_count} null values")
    
    # Statistics
    stats['total_segments'] = len(segments_df)
    stats['unique_conversations'] = segments_df['CONVERSATION_ID'].nunique() if 'CONVERSATION_ID' in segments_df.columns else 0
    stats['segments_with_tools'] = sum(segments_df['ACTUAL_TOOLS_CALLED'].apply(
        lambda x: len(json.loads(x)) > 0 if x else False
    )) if 'ACTUAL_TOOLS_CALLED' in segments_df.columns else 0
    stats['segments_without_execution_id'] = sum(
        segments_df['EXECUTION_ID'].isnull() | (segments_df['EXECUTION_ID'] == '')
    ) if 'EXECUTION_ID' in segments_df.columns else 0
    
    # Warnings
    if stats.get('segments_without_execution_id', 0) > 0:
        warnings.append(f"{stats['segments_without_execution_id']} segments have no execution_id")
    
    if stats.get('segments_with_tools', 0) == 0:
        warnings.append("No segments have detected tools")
    
    # Check for duplicate message IDs
    if 'MESSAGE_ID' in segments_df.columns:
        duplicate_count = segments_df['MESSAGE_ID'].duplicated().sum()
        if duplicate_count > 0:
            warnings.append(f"{duplicate_count} duplicate message IDs found")
    
    is_valid = len(errors) == 0
    
    return {
        'is_valid': is_valid,
        'errors': errors,
        'warnings': warnings,
        'stats': stats
    }


def get_message_segment_summary(segments_df: pd.DataFrame) -> str:
    """
    Generate a human-readable summary of message segments.
    
    Args:
        segments_df: DataFrame with message segments
    
    Returns:
        Summary string
    """
    if segments_df.empty:
        return "No message segments generated"
    
    validation = validate_message_segment_conversion(segments_df)
    stats = validation['stats']
    
    summary_lines = [
        f"Message Segments Summary:",
        f"  Total Segments: {stats.get('total_segments', 0)}",
        f"  Unique Conversations: {stats.get('unique_conversations', 0)}",
        f"  Segments with Tools: {stats.get('segments_with_tools', 0)}",
        f"  Segments without Execution ID: {stats.get('segments_without_execution_id', 0)}"
    ]
    
    if validation['warnings']:
        summary_lines.append(f"  ⚠️ Warnings: {len(validation['warnings'])}")
        for warning in validation['warnings']:
            summary_lines.append(f"    - {warning}")
    
    if validation['errors']:
        summary_lines.append(f"  ❌ Errors: {len(validation['errors'])}")
        for error in validation['errors']:
            summary_lines.append(f"    - {error}")
    
    return "\n".join(summary_lines)

