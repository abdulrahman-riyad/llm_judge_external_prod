"""
Helper Functions for LLM Analysis

Common utility functions used by XML/message converters and other modules.

ENVIRONMENT AGNOSTIC:
This module uses only pandas and standard Python - no Snowpark dependencies.
It works in all environments:
- Inside Snowflake (via Snowpark)
- Google Colab
- GitHub Actions
- Local development
"""

import pandas as pd
import json
import re
from snowflake_llm_config import get_snowflake_llm_departments_config

def get_execution_id_map(conversations_df, department_name):
    """
    Get execution_id map for conversations
    """
    execution_id_map = {}
    # Compute EXECUTION_ID per conversation: first non-null EXECUTION_ID where TARGET_SKILL_PER_MESSAGE
    # is in bot or agent skills (bot messages prioritized), sorted by MESSAGE_SENT_TIME
    execution_id_map = {}
    try:
        dept_cfg_all = get_snowflake_llm_departments_config()
        bot_skills = dept_cfg_all.get(department_name, {}).get('bot_skills', [])
        agent_skills = dept_cfg_all.get(department_name, {}).get('agent_skills', [])
        combined_skills = list(dict.fromkeys(list(bot_skills) + list(agent_skills)))

        # Debug: show available columns
        try:
            print(f"    🧩 conversations_df columns: {list(conversations_df.columns)}")
        except Exception:
            pass

        cols_upper = {c.upper(): c for c in conversations_df.columns}
        id_col = 'conversation_id' if 'conversation_id' in conversations_df.columns else cols_upper.get('CONVERSATION_ID')
        time_col = cols_upper.get('MESSAGE_SENT_TIME')
        skill_col = cols_upper.get('TARGET_SKILL_PER_MESSAGE')
        exec_col = 'execution_id' if 'execution_id' in conversations_df.columns else cols_upper.get('EXECUTION_ID')

        # Debug: which required columns resolved
        print(f"    🔎 EXECUTION_ID mapping requires -> id:{id_col}, time:{time_col}, skill:{skill_col}, exec:{exec_col}")

        if id_col and time_col and skill_col and exec_col:
            tmp = conversations_df[[id_col, time_col, skill_col, exec_col]].copy()
            # Filter rows with matching bot/agent skill and non-null execution id
            tmp = tmp[(tmp[skill_col].isin(combined_skills)) & (tmp[exec_col].notna()) & (tmp[exec_col].astype(str) != '')]
            if not tmp.empty:
                # Sort by time then take first per conversation
                try:
                    tmp[time_col] = pd.to_datetime(tmp[time_col], errors='coerce')
                except Exception:
                    pass
                # Prioritize bot skill messages first, then by time ascending within conversation
                tmp = tmp.assign(__is_bot_skill=tmp[skill_col].isin(bot_skills))
                tmp = tmp.sort_values([id_col, '__is_bot_skill', time_col], ascending=[True, False, True])
                first_rows = tmp.groupby(id_col, as_index=False).first()
                execution_id_map = {
                    str(r[id_col]): (
                        str(r[exec_col]).strip().split('.', 1)[0]
                        if str(r[exec_col]).strip().replace('.', '', 1).isdigit()
                        else str(r[exec_col]).strip()
                    )
                    for _, r in first_rows.iterrows()
                }
            print(f"    🔗 EXECUTION_ID mapping prepared for {len(execution_id_map)} conversations")
        else:
            print("    ⚠️  Skipping EXECUTION_ID mapping: required columns not present in conversations_df")
    except Exception as e:
        print(f"    ⚠️  Failed to prepare EXECUTION_ID mapping: {str(e)}")
    
    return execution_id_map

def safe_json_loads(json_str):
    """Safely parse JSON strings with error handling"""
    try:
        if pd.isna(json_str):
            return {}
        # Remove invisible unicode characters and strip whitespace
        cleaned = str(json_str).replace('\u202f', '').replace('\xa0', '').strip()
        
        # Handle empty or whitespace-only strings
        if not cleaned:
            return {}
        
        # Try strict JSON parse first; on failure, remove trailing commas before } or ] and retry
        try:
            return json.loads(cleaned)
        except Exception:
            cleaned_tc = re.sub(r',\s*([}\]])', r'\1', cleaned)
            return json.loads(cleaned_tc)
    except json.JSONDecodeError:
        return "INVALID_JSON"
    except Exception:
        return "INVALID_JSON"


def get_tool_name_and_response(conv_df, text_value):
    """
    Return (tool_name, tool_response) from a tool-related TEXT payload.

    Strategy:
      1) Try extracting (tool_name, tool_call_id) via extract_tool_name_and_call_id();
         if found, look up the matching tool response content in conv_df.
      2) If not found or response unavailable, fall back to extract_tool_name_and_arguments();
         treat the arguments object as the response.

    Args:
      conv_df: pandas.DataFrame of the conversation (must include MESSAGE_TYPE/TEXT for lookup)
      text_value: dict or JSON string for the 'tool' message

    Returns:
      (tool_name, tool_response) where tool_response is either the tool response content or
      the arguments dict. Returns (None, None) if not determinable.
    """
    try:
        tool_name, tool_call_id, tool_args = extract_tool_name_and_call_id(text_value)
        if tool_name and tool_call_id:
            response_content = find_tool_response_content_in_conv(conv_df, tool_call_id)
            if response_content is not None:
                return tool_name, response_content, tool_args

        # Fallback to arguments-as-response
        tool_name_alt, arguments_obj = extract_tool_name_and_arguments(text_value)
        if tool_name_alt and isinstance(arguments_obj, dict):
            return tool_name_alt, None , arguments_obj
        return None, None, None
    except Exception:
        return None, None, None

def extract_tool_name_and_call_id(text_value):
    """
    Parse a 'tool' message TEXT and extract (tool_name, tool_call_id, tool_args).
    Accepts dict or JSON string; returns (None, None, None) if not found.
    """
    try:
        parsed = text_value if isinstance(text_value, dict) else safe_json_loads(text_value)
        if not isinstance(parsed, dict):
            return None, None, None

        tool_calls = parsed.get('tool_calls')
        if isinstance(tool_calls, list) and tool_calls:
            call = tool_calls[0]
            if isinstance(call, dict):
                tool_name = call.get('name') or call.get('tool_name')
                tool_call_id = call.get('id') or call.get('tool_call_id')
                tool_args = call.get('args') or call.get('arguments')
                return tool_name, tool_call_id, tool_args
        return None, None, None
    except Exception:
        return None, None, None


def find_tool_response_content_in_conv(conv_df, tool_call_id):
    """
    Search the conversation DataFrame for a matching 'tool response' message by tool_call_id
    and return the exact 'content' value from its TEXT JSON.
    """
    if not tool_call_id:
        return None
    try:
        for _, r in conv_df.iterrows():
            msg_type = str(r.get('MESSAGE_TYPE', '')).lower()
            if msg_type != 'tool response':
                continue
            text_val = r.get('TEXT')
            parsed = text_val if isinstance(text_val, dict) else safe_json_loads(text_val)
            if not isinstance(parsed, dict):
                continue
            match_id = parsed.get('tool_call_id')
            if match_id and str(match_id) == str(tool_call_id):
                return parsed.get('content')
        return None
    except Exception:
        return None


def extract_tool_name_and_arguments(text_value):
    """
    Parse a 'tool' message TEXT and extract (tool_name, tool_arguments).
    Accepts dict or JSON string; returns (None, None) if not found.
    """
    try:
        parsed = text_value if isinstance(text_value, dict) else safe_json_loads(text_value)
        if not isinstance(parsed, dict):
            return None, None

        name_exists = 'name' in parsed and isinstance(parsed.get('name'), str) and parsed.get('name')
        arguments_obj = parsed.get('arguments') or parsed.get('args')
        arguments_is_object = isinstance(arguments_obj, dict)
        if name_exists and arguments_is_object:
            return parsed.get('name'), parsed.get('arguments') or parsed.get('args')
        return None, None
    except Exception:
        return None, None

