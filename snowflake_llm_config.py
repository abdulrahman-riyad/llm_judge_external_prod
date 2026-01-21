"""
LLM Configuration Module for Snowflake Chat Analysis
Defines department configurations, prompts, and model preferences

This module is designed to work in both Snowflake and external environments:
- In Snowflake: Full functionality including metrics calculation functions
- Externally (GitHub Actions/Colab): Config data only, metrics functions as strings

The conditional import allows this module to be used for configuration 
retrieval even when snowflake.snowpark is not available.
"""

from prompts import *

# =============================================================================
# CONDITIONAL METRICS IMPORT
# =============================================================================
# Try to import metrics_calc module. If it fails (due to snowpark dependency),
# we still have access to all configuration data - just not the actual
# metrics calculation functions.

_METRICS_CALC_AVAILABLE = False
_METRICS_FUNCTIONS = {}

try:
    from snowflake_llm_metrics_calc import *
    _METRICS_CALC_AVAILABLE = True
    
    # Store ALL function references used in get_metrics_configuration()
    # This allows dynamic lookup by function name when running in external environments
    _METRICS_FUNCTIONS = {
        # Core metrics functions
        'calculate_agent_intervention_metrics': calculate_agent_intervention_metrics,
        'calculate_at_african_policy_violation_metrics': calculate_at_african_policy_violation_metrics,
        'calculate_at_african_tool_metrics': calculate_at_african_tool_metrics,
        'calculate_at_african_transfer_metrics': calculate_at_african_transfer_metrics,
        'calculate_backed_out_metrics': calculate_backed_out_metrics,
        'calculate_call_request_metrics': calculate_call_request_metrics,
        'calculate_call_request_metrics_cc_resolvers': calculate_call_request_metrics_cc_resolvers,
        'calculate_cc_delighters_categorizing_metrics': calculate_cc_delighters_categorizing_metrics,
        'calculate_cc_delighters_missed_tool_percentage': calculate_cc_delighters_missed_tool_percentage,
        'calculate_cc_delighters_transfers_due_to_escalations': calculate_cc_delighters_transfers_due_to_escalations,
        'calculate_cc_delighters_unsatisfactory_policy_percentage': calculate_cc_delighters_unsatisfactory_policy_percentage,
        'calculate_cc_resolvers_categorizing_metrics': calculate_cc_resolvers_categorizing_metrics,
        'calculate_cc_resolvers_transfers_due_to_escalations': calculate_cc_resolvers_transfers_due_to_escalations,
        'calculate_cc_resolvers_unclear_policy_percentage': calculate_cc_resolvers_unclear_policy_percentage,
        'calculate_cc_resolvers_unsatisfactory_policy_percentage': calculate_cc_resolvers_unsatisfactory_policy_percentage,
        'calculate_cc_sales_policy_violation_metrics': calculate_cc_sales_policy_violation_metrics,
        'calculate_clarification_percentage': calculate_clarification_percentage,
        'calculate_clarity_score_percentage': calculate_clarity_score_percentage,
        'calculate_client_mention_another_maid_percentage': calculate_client_mention_another_maid_percentage,
        'calculate_client_suspecting_ai_percentage': calculate_client_suspecting_ai_percentage,
        'calculate_closing_message_metrics': calculate_closing_message_metrics,
        'calculate_complexity_violation_percentage': calculate_complexity_violation_percentage,
        'calculate_de_escalation_success_percentage': calculate_de_escalation_success_percentage,
        'calculate_delighters_missing_policy_metrics': calculate_delighters_missing_policy_metrics,
        'calculate_dissatisfaction_percentage': calculate_dissatisfaction_percentage,
        'calculate_doctor_agent_intervention_percentage': calculate_doctor_agent_intervention_percentage,
        'calculate_doctors_health_check_response_percentage': calculate_doctors_health_check_response_percentage,
        'calculate_doctors_insurance_complaints_percentage': calculate_doctors_insurance_complaints_percentage,
        'calculate_doctors_missing_policy_metrics': calculate_doctors_missing_policy_metrics,
        'calculate_doctors_missing_tool_percentage': calculate_doctors_missing_tool_percentage,
        'calculate_doctors_transfer_escalation_percentage': calculate_doctors_transfer_escalation_percentage,
        'calculate_doctors_transfer_known_flow_percentage': calculate_doctors_transfer_known_flow_percentage,
        'calculate_doctors_wrong_tool_percentage': calculate_doctors_wrong_tool_percentage,
        'calculate_document_request_failure_chat_percentage': calculate_document_request_failure_chat_percentage,
        'calculate_document_request_failure_percentage': calculate_document_request_failure_percentage,
        'calculate_exceptions_granted_percentage': calculate_exceptions_granted_percentage,
        'calculate_facephoto_analysis_metrics': calculate_facephoto_analysis_metrics,
        'calculate_false_promises_percentage': calculate_false_promises_percentage,
        'calculate_ftr_percentage': calculate_ftr_percentage,
        'calculate_gcc_analysis_metrics': calculate_gcc_analysis_metrics,
        'calculate_grammar_metrics': calculate_grammar_metrics,
        'calculate_gulf_maids_clarification_metrics': calculate_gulf_maids_clarification_metrics,
        'calculate_gulf_maids_tool_metrics': calculate_gulf_maids_tool_metrics,
        'calculate_incorrect_assessment_1_metrics': calculate_incorrect_assessment_1_metrics,
        'calculate_incorrect_assessment_2_metrics': calculate_incorrect_assessment_2_metrics,
        'calculate_kenyan_flow_order_metrics': calculate_kenyan_flow_order_metrics,
        'calculate_kenyan_profile_update_metrics': calculate_kenyan_profile_update_metrics,
        'calculate_legal_metrics': calculate_legal_metrics,
        'calculate_legitimacy_metrics': calculate_legitimacy_metrics,
        'calculate_mfa_analysis_metrics': calculate_mfa_analysis_metrics,
        'calculate_misprescription_percentage': calculate_misprescription_percentage,
        'calculate_missing_policy_metrics': calculate_missing_policy_metrics,
        'calculate_negative_tool_response_metrics': calculate_negative_tool_response_metrics,
        'calculate_overall_percentages': calculate_overall_percentages,
        'calculate_passport_analysis_metrics': calculate_passport_analysis_metrics,
        'calculate_poke_check_metrics': calculate_poke_check_metrics,
        'calculate_policy_escalation_percentage': calculate_policy_escalation_percentage,
        'calculate_promise_no_tool_triggered_percentage': calculate_promise_no_tool_triggered_percentage,
        'calculate_prospect_agent_involvement_metrics': calculate_prospect_agent_involvement_metrics,
        'calculate_prospect_chats_shadowed_metrics': calculate_prospect_chats_shadowed_metrics,
        'calculate_prospect_cost_metrics': calculate_prospect_cost_metrics,
        'calculate_prospect_dropoff_metrics': calculate_prospect_dropoff_metrics,
        'calculate_prospect_fully_handled_by_bot_metrics': calculate_prospect_fully_handled_by_bot_metrics,
        'calculate_prospect_no_reply_metrics': calculate_prospect_no_reply_metrics,
        'calculate_prospect_poke_reengagement_metrics': calculate_prospect_poke_reengagement_metrics,
        'calculate_prospect_required_tools_not_called_metrics': calculate_prospect_required_tools_not_called_metrics,
        'calculate_prospect_routing_identification_metrics': calculate_prospect_routing_identification_metrics,
        'calculate_prospect_total_transfers_metrics': calculate_prospect_total_transfers_metrics,
        'calculate_prospect_transfer_correctness_metrics': calculate_prospect_transfer_correctness_metrics,
        'calculate_questioning_legalities_metrics': calculate_questioning_legalities_metrics,
        'calculate_repetition_metrics': calculate_repetition_metrics,
        'calculate_routing_bot_false_promises_percentage': calculate_routing_bot_false_promises_percentage,
        'calculate_sales_transer_percentage': calculate_sales_transer_percentage,
        'calculate_threatening_case_identifier_percentage': calculate_threatening_case_identifier_percentage,
        'calculate_threatening_percentage': calculate_threatening_percentage,
        'calculate_tool_eval_metrics': calculate_tool_eval_metrics,
        'calculate_total_chats': calculate_total_chats,
        'calculate_unclear_policy_metrics': calculate_unclear_policy_metrics,
        'calculate_unnecessary_clinic_percentage': calculate_unnecessary_clinic_percentage,
        'calculate_unsatisfactory_policy_percentage': calculate_unsatisfactory_policy_percentage,
        'calculate_weighted_nps_per_department': calculate_weighted_nps_per_department,
        'calculate_wrong_answer_metrics': calculate_wrong_answer_metrics,
        'calculate_wrong_answer_percentage': calculate_wrong_answer_percentage,
        # Report generation functions
        'analyze_categorizing_data_snowflake': analyze_categorizing_data_snowflake,
        'generate_at_filipina_tool_summary_report': generate_at_filipina_tool_summary_report,
        'generate_cancellation_request_retraction_breakdown_report': generate_cancellation_request_retraction_breakdown_report,
        'generate_cc_delighters_wrong_tool_summary_report': generate_cc_delighters_wrong_tool_summary_report,
        'generate_mv_resolvers_missing_tool_summary_report': generate_mv_resolvers_missing_tool_summary_report,
        'generate_mv_resolvers_wrong_tool_summary_report': generate_mv_resolvers_wrong_tool_summary_report,
        'generate_policy_transfer_summary_report': generate_policy_transfer_summary_report,
        'generate_replacement_request_retraction_breakdown_report': generate_replacement_request_retraction_breakdown_report,
        'generate_request_retraction_summary_report': generate_request_retraction_summary_report,
        'generate_routing_bot_tool_summary_report': generate_routing_bot_tool_summary_report,
        'generate_tool_summary_report': generate_tool_summary_report,
        'get_prospect_llm_model_config_metrics': get_prospect_llm_model_config_metrics,
    }
except ImportError as e:
    # Running in external environment (GitHub Actions, Colab) without snowpark
    # Configuration data is still available, just not the calculation functions
    import warnings
    warnings.warn(
        f"snowflake_llm_metrics_calc not available (likely no snowpark): {e}. "
        "Config functions will work, but metrics calculation functions won't be available."
    )


def is_metrics_calc_available() -> bool:
    """Check if metrics calculation functions are available."""
    return _METRICS_CALC_AVAILABLE


def get_metrics_function(func_name: str):
    """
    Get a metrics calculation function by name.
    
    Args:
        func_name: Name of the function (e.g., 'calculate_ftr_percentage')
    
    Returns:
        The function if available, None otherwise
    """
    return _METRICS_FUNCTIONS.get(func_name)


# ==================== TOOL NAME MAPPING CONFIGURATIONS ====================

def get_mv_resolvers_tool_name_mapping():
    """
    Return the tool name mapping configuration for MV Resolvers.
    Maps variant tool names to their standardized general names and definitions.
    
    Returns:
        dict: {variant_tool_name: (general_tool_name, tool_info)}
    """
    return {
        # ATM Relocation
        'ATM_relocation': ('ATM Relocation', 'This tool handles relocating the maid\'s ATM Card to the necessary branch as per the customer\'s request'),
        
        # Ansari Salary Statement
        'Ansari Salary Statement tool': ('Ansari Salary Statement', 'This tool generates a salary statement from Ansari to be used as proof of salary payment to the maid.'),
        'Get Ansari Salary Statement': ('Ansari Salary Statement', 'This tool generates a salary statement from Ansari to be used as proof of salary payment to the maid.'),
        'Get Ansari Salary Statement tool': ('Ansari Salary Statement', 'This tool generates a salary statement from Ansari to be used as proof of salary payment to the maid.'),
        'get_ansari_salary_statement': ('Ansari Salary Statement', 'This tool generates a salary statement from Ansari to be used as proof of salary payment to the maid.'),
        'salary_statement_generation': ('Ansari Salary Statement', 'This tool generates a salary statement from Ansari to be used as proof of salary payment to the maid.'),
        
        # Changing Payment Method to CC
        'ChangingPaymentMethodtoCreditCard': ('Changing Payment Method to CC', 'This tool handles changing the customer\'s payment method from DD to CC.'),
        'ChangingPaymentMethodDDToCC': ('Changing Payment Method to CC', 'This tool handles changing the customer\'s payment method from DD to CC.'),
        
        # Send CC Link
        'SendCCLink': ('Send CC Link', 'This tool generates a new Credit Card link for a client\'s payments in case they would like to pay a payment via a new Credit Card link'),
        
        # Send DD Form
        'SendDDForm': ('Send DD Form', 'This tool sends the customer the DD links to sign and set up his direct debits'),
        
        # Transfer Chat (multiple variants)
        'TransferTool': ('Transfer Chat', 'This tool transfers the conversation to the necessary skill based on context'),
        'Transfer_Chat': ('Transfer Chat', 'This tool transfers the conversation to the necessary skill based on context'),
        'transfer tool': ('Transfer Chat', 'This tool transfers the conversation to the necessary skill based on context'),
        'transfer_agent': ('Transfer Chat', 'This tool transfers the conversation to the necessary skill based on context'),
        'ConvertChatToGPTMVRESOLVERS': ('Transfer Chat', 'This tool transfers the conversation to the necessary skill based on context'),
        'TransferTool to MV_Resolvers_Seniors': ('Transfer Chat', 'This tool transfers the conversation to the necessary skill based on context'),
        'transfer_conversation': ('Transfer Chat', 'This tool transfers the conversation to the necessary skill based on context'),
        'transfer_tool': ('Transfer Chat', 'This tool transfers the conversation to the necessary skill based on context'),
        'transfer_conversation to GPT_CC_Prospect': ('Transfer Chat', 'This tool transfers the conversation to the necessary skill based on context'),
        
        # Contract Cancellation
        'contract_cancellation': ('Contract Cancellation', 'This tool schedules the client for cancellation based on his requested cancellation date.'),
        
        # Contract Verification
        'contract_verification': ('Contract Verification', 'This tool is used for contract verification of the maid.'),
        
        # Early Visa Renewal
        'early_visa_renewal': ('Early Visa Renewal', 'This tool triggers early visa renewal for the maid the client is talking about'),
        
        # Enable Visa Proactive Message
        'enable_visa_proactive_message': ('Enable Visa Proactive Message', 'This tool enables visa pro-active messages to update the client automatically of his visa process updates if requested'),
        
        # Generate Travel NOC
        'generate_travel_NOC_alone': ('Generate Travel NOC - Alone', 'This tool generates a travel NOC if the maid is travelling alone to her home country'),
        'generate_travel_NOC_with_client': ('Generate Travel NOC - With Client', 'This tool generates a travel NOC if the maid is travelling with the client.'),
        'generate_travel_NOC_with_someone_else': ('Generate Travel NOC - With Someone Else', 'This tool issues a travel noc for the maid to travel with a companion that is not the client'),
        
        # Get Branches
        'getBranches': ('Get Branches', 'This tool gets the branches of Ansari Exchange within a certain Emirate for the sake of relocation'),
        
        # MBR
        'mbr': ('MBR', 'This tool handles re-scheduling the medical/biometric appointment for the maid'),
        
        # Nepalese to Nepal
        'nepalese_to_nepal': ('Nepalese to Nepal', 'This tool generates the travel documents for nepalese maids to travel to their home country.'),
        
        # Open Complaint To
        'open_complaint_to': ('Open Complaint To', 'This tool opens any complaint on the customer\'s profile for the necessary department needed.'),
        
        # Refund Payment
        'refund_payment_tool': ('Refund Payment', 'This tool is used to refund a payment for a client with a current cap of 300 AED.'),
        
        # Select Contract
        'select_contract': ('Select Contract', 'This tool handles selecting a new contract for the sake of handling an inquiry related to a different maid than the one that was mentioned initially.'),
        
        # Send a Document (multiple variants)
        'send_a_document': ('Send a Document', 'This tool handles sending any requested document to the client/maid.'),
        'send_a_document1': ('Send a Document', 'This tool handles sending any requested document to the client/maid.'),
        'send_a_document tool': ('Send a Document', 'This tool handles sending any requested document to the client/maid.'),
        'Send_Document': ('Send a Document', 'This tool handles sending any requested document to the client/maid.'),
        
        # Travel to Lebanon
        'travel_to_lebanon': ('Travel to Lebanon', 'This tool handles requsting a travel to Lebanon visa for the customer\'s maid'),
        'traveltolebanon': ('Travel to Lebanon', 'This tool handles requsting a travel to Lebanon visa for the customer\'s maid'),
        
        # Update Customer And Maid Info
        'update_customer_and_maid_info': ('Update Customer And Maid Info', 'This tool updates the client and maid info in the family details section such as Spouse Name, Client number, Maid number, etc.'),
        
        # UPL (multiple variants)
        'upl': ('UPL', 'This tool handles uploading the maid\'s document to the maid profile for the visa process (Missing face photo, passport photo, etc)'),
        'upl tool': ('UPL', 'This tool handles uploading the maid\'s document to the maid profile for the visa process (Missing face photo, passport photo, etc)'),
        'UploadMaidFacePhotoToERP': ('UPL', 'This tool handles uploading the maid\'s document to the maid profile for the visa process (Missing face photo, passport photo, etc)'),
        
        # Create Taxi Work Order
        'create_taxi_work_order': ('Create Taxi Work Order', 'This tool is used to create a taxi work order for the customer'),
        'Taxi_Booking': ('Create Taxi Work Order', 'This tool is used to create a taxi work order for the customer'),
        
        # Medical Bio Single Day
        'medical_bio_single_day': ('Medical Bio Single Day', 'This tool allows the customer to have medical and bio to be done for his maid in the same day'),
        
        # SOA Generation
        'soa_generation': ('SOA Generation', 'This tool generates a Statement Of Account of the client\'s payments based on a given time range.'),
        
        # Undo Cancellation
        'undo_cancellation': ('Undo Cancellation', 'This tool undo\'s the customer cancellation'),
        
        # Careem Box Delivery to Family
        'CareemBox - Delivery to Family': ('Careem Box Delivery to Family', 'This tool is used to send a careem box to the client\'s chosen address.'),
        
        # Record Referral
        'record_referral': ('Record Referral', 'This tool is used to record a referral'),
        
        # Maid Documents Uploading
        'maid_documents_uploading': ('Maid Documents Uploading', 'This tool sends a document relating to the maid and we need to upload it in her profile'),
        
        # Changing Payment Method CC Manual To CC Auto
        'ChangingPaymentMethodCCManualToCCAuto': ('Changing Payment Method CC Manual To CC Auto', 'This tool switches the customer payment method from paying the company through direct debit to paying via debit or credit card not vice versa'),
        
        # Tools with no definition (-)
        'postponed_contract_reactivation': ('Postponed Contract Reactivation', ''),
        'SendDocumentSampleToCustomer': ('Send Document Sample To Customer', ''),
        'LastMVSalaryRelease': ('Last MV Salary Release', ''),
        'update_google_review': ('Update Google Review', ''),
        'Travel_To_Philippines': ('Travel To Philippines', ''),
        'AddManagerNote': ('Add Manager Note', ''),
        'CaptureDataFromCustomer': ('Capture Data From Customer', ''),
        'SetBotStage': ('Set Bot Storage', ''),
        'SendExpectationMessageViaSupportNumber': ('Send Expectation Message Via Support Number', ''),
        'UploadMultipleCustomerInputsToERP': ('Upload Multiple Customer Inputs To ERP', ''),
    }


def get_general_tool_name_and_info(tool_name, department='MV_Resolvers'):
    """
    Get the general tool name and info for a given tool name.
    If no mapping exists, returns the original tool name with empty info.
    
    Args:
        tool_name (str): The original tool name from the data
        department (str): The department (currently only supports MV_Resolvers)
    
    Returns:
        tuple: (general_tool_name, tool_info)
    """
    if department == 'MV_Resolvers':
        mapping = get_mv_resolvers_tool_name_mapping()
        return mapping.get(tool_name, (tool_name, ''))
    else:
        # For other departments, return as-is
        return (tool_name, '')


# ==================== BASE DEPARTMENT CONFIGURATIONS ====================

def get_snowflake_base_departments_config():
    """
    Base department configuration from existing snowflake_phase2_core_analytics.py
    Maps department names to their bot_skills, agent_skills, and Snowflake table names.
    """
    return {
        'Routing_Bot': {
            'bot_skills': ['MULTIPLE_CONTRACT_DETECTOR'],
            'agent_skills': [],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',
            'skill_filter': 'multiple_contract_detector',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'Routing Bot'
        },
        'CC_Resolvers': {
            'bot_skills': ['GPT_CC_RESOLVERS'],
            'agent_skills': ['CC_RESOLVERS_AGENTS', 'CC_RESOLVER_SUPERVISOR', 'GPT_CC_RESOLVERS_SHADOWER'],
            'table_name': 'SILVER.CHAT_EVALS.CC_CLIENT_CHATS',
            'skill_filter': 'gpt_cc_resolvers',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'ChatGPT CC Resolvers'
        },
        'MV_Resolvers': {
            'bot_skills': ['GPT_MV_RESOLVERS'],
            'agent_skills': ['MV_RESOLVERS_SENIORS', 'MV_CALLERS', 'MV_RESOLVERS_MANAGER', 
                           'GPT_MV_RESOLVERS_SHADOWERS', 'GPT_MV_RESOLVERS_SHADOWERS_MANAGER', 'Pre_R_Visa_Retention'],
            'table_name': 'SILVER.CHAT_EVALS.MV_CLIENTS_CHATS',
            'skill_filter': 'gpt_mv_resolvers',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'MV Resolvers V2'
        },
        'CC_Sales': {
            'bot_skills': ['GPT_CC_PROSPECT'],
            'agent_skills': ['GPT CC Shadowers'],
            'table_name': 'SILVER.CHAT_EVALS.CC_SALES_CHATS',
            'skill_filter': 'gpt_cc_prospect',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'CC Enchanters v2 Agent'
        },
        'MV_Sales': {
            'bot_skills': ['GPT_MV_PROSPECT', 'GPT_MV_PROSPECT_N8N'],
            'agent_skills': ['CHATGPT_SALES_SHADOWERS'],
            'table_name': 'SILVER.CHAT_EVALS.MV_SALES_CHATS',
            'skill_filter': 'gpt_mv_prospect',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'MV - Sales - Sally'
        },
        'MV_Delighters': {
            'bot_skills': ['GPT_MV_DELIGHTERS'],
            'agent_skills': [
                'MV_RESOLVERS_SENIORS', 'MV_CALLERS','MV_RESOLVERS_MANAGER','GPT_MV_RESOLVERS_SHADOWERS','GPT_MV_RESOLVERS_SHADOWERS_MANAGER','Pre_R_Visa_Retention'
            ],
            'table_name': 'SILVER.CHAT_EVALS.DELIGHTERS_CHATS',
            'skill_filter': 'gpt_delighters',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'Delighters Agent '
        },
        'CC_Delighters': {
            'bot_skills': ['GPT_CC_DELIGHTERS'],
            'agent_skills': ['GPT_CC_DELIGHTERS_SHADOWERS','CC_DELIGHTER_ETHIOPIAN','CC_DELIGHTER_OROMO','CC_DELIGHTER_SENIOR','CC_DELIGHTER_SUPERVISOR','DELIGHTER_MANAGER'],
            'table_name': 'SILVER.CHAT_EVALS.DELIGHTERS_CHATS',  # Update with actual table name
            'skill_filter': 'gpt_delighters',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'Delighters Agent '
        },
        'Doctors': {
            'bot_skills': ['GPT_Doctors'],
            'agent_skills': ['Doctor'],
            'table_name': 'SILVER.CHAT_EVALS.DOCTORS_CHATS',
            'skill_filter': 'gpt_doctors',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'Doctor\'s assistant'
        },
        'AT_Filipina': {
            'bot_skills': [
               'Filipina_in_PHl_Pending_Valid_Visa','Filipina_in_PHl_Pending_valid_visa','Filipina_Outside_Pending_Facephoto',
                'Filipina_Outside_Pending_Passport', 'Filipina_Outside_Pending_Ticket', 'Filipina_Outside_Ticket_Booked',
                'Filipina_in_PHl_Pending_Facephoto', 'Filipina_in_PHl_Pending_OEC_From_Company',
                'Filipina_in_PHl_Pending_OEC_From_maid', 'Filipina_in_PHl_Pending_Passport', 'Filipina_in_PHl_Pending_Ticket',
                'Filipina_in_PHl_Pending_valid_visa', 'Filipina_in_PHl_Ticket_Booked',
               'Filipina_Outside_UAE_Pending_Joining_Date', 'Filipina_Outside_Upcoming_Joining',
                'GPT_MAIDSAT_FILIPINA_UAE'
            ],
            'agent_skills': [
                'NUDGERS_REPETITIVE', 'GPT_FILIPINA_SHADOWERS', 'Nudger_TaxiBooking',
                'Nudgers_agents', 'AIRPORT_HUSTLER'
            ],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',
            'skill_filter': 'filipina_outside',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'Maids Line'
        },
        'AT_Filipina_In_PHL': {
            'bot_skills': [
                'Filipina_in_PHl_Pending_Valid_Visa','Filipina_in_PHl_Pending_valid_visa', 'Filipina_in_PHl_Pending_Passport', 'Filipina_in_PHl_Pending_Facephoto',
                'Filipina_in_PHl_Pending_OEC_From_Company', 'Filipina_in_PHl_Pending_OEC_From_maid', 'Filipina_in_PHl_Pending_Ticket',
                'Filipina_in_PHl_Ticket_Booked'
            ],
            'agent_skills': [
                'NUDGERS_REPETITIVE', 'GPT_FILIPINA_SHADOWERS', 'Nudger_TaxiBooking',
                'Nudgers_agents', 'AIRPORT_HUSTLER', 'PHILIPPINES_FILIPINA_SHADOWERS'
            ],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',  # Shared table
            'skill_filter': 'filipina_outside',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'Maids Line'
        },
        'AT_Filipina_Outside_UAE': {
            'bot_skills': [
                'Filipina_Outside_UAE_Pending_Joining_Date',
                'Filipina_Outside_Upcoming_Joining', 'Filipina_Outside_Pending_Passport', 'Filipina_Outside_Pending_Ticket',
                'Filipina_Outside_Ticket_Booked','Filipina_Outside_Pending_Facephoto'
            ],
            'agent_skills': [
                'NUDGERS_REPETITIVE', 'GPT_FILIPINA_SHADOWERS', 'Nudger_TaxiBooking',
                'Nudgers_agents', 'AIRPORT_HUSTLER', 'OUTSIDE_FILIPINA_SHADOWERS'
            ],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',  # Shared table
            'skill_filter': 'filipina_in_phl',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'Maids Line'
        },
        'AT_Filipina_Inside_UAE': {
            'bot_skills': [
                 'GPT_MAIDSAT_FILIPINA_UAE'
            ],
            'agent_skills': [
                'NUDGERS_REPETITIVE', 'GPT_FILIPINA_SHADOWERS', 'Nudger_TaxiBooking',
                'Nudgers_agents', 'AIRPORT_HUSTLER', 'Nudger_TaxiBooking'
            ],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',  # Shared table
            'skill_filter': 'filipina_inside',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'Maids Line'
        },
        'AT_African': {
            'bot_skills': [
                'MAIDSAT_AFRICAN_GPT', 'GPT_MAIDSAT_AFRICAN_KENYA', 
                'GPT_MAIDSAT_AFRICAN_OUTSIDE', 'GPT_MAIDSAT_AFRICAN_UAE', 'Kenyan WP Approved', 'Kenyan Assessment', 'Kenyan Client Scenario', 'Kenyan Attested Pending WP', 'Kenyan Passport Collection'
            ],
            'agent_skills': [
                'AFRICAN_NUDGER', 'Kenyan_Attestation_Hustling', 'Kenyan_PreAttestation',
                'Nudgers_Repetitive_Kenyan', 'AFRICAN_NUDGER'
            ],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',
            'skill_filter': 'maidsat_africa',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'Maids Line'
        },
        'AT_Ethiopian': {
            'bot_skills': [
                'MAIDSAT_ETHIOPIAN_GPT',
                'GPT_MAIDSAT_ETHIOPIA_ETHIOPIA',
                'GPT_MAIDSAT_ETHIOPIA_OUTSIDE',
                'GPT_MAIDSAT_ETHIOPIA_UAE',
                'Ethiopian Assessment',
                'Ethiopian Passed Question Assessment',
                'Ethiopian Failed Question Assessment',
                'Ethiopian Client Scenario',
                'Ethiopian Sent video',
                'Ethiopian Failed Client Scenario',
                'Ethiopian Applicant Passed Video',
                'Ethiopian Applicant Failed Video',
                'Ethiopian Profile Picture Collection',
                'Ethiopian Passport Collection',
                'Ethiopian Pending operator visit',
                'Ethiopian OP Assessment',
                'Ethiopian OP Passed Questions',
                'Ethiopian OP Failed Questions',
                'Ethiopian OP Client Scenario',
                'Ethiopian OP Sent Video',
                'Ethiopian OP Failed Client Scenario',
                'Ethiopian OP Passed Video',
                'Ethiopian OP Failed Video',
                'Ethiopian Invalid Passport',
                'Ethiopian LAWP Maids'
            ],
            'agent_skills': [
                'ETHIOPIAN_NUDGER', 'SCREENERS AGENTS'
            ],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',
            'skill_filter': 'maidsat_ethiopia',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'Maids Line'
        },
        'Gulf_maids': {
            'bot_skills': ['Filipina_in_PHl_NO_AV'],
            'agent_skills': [],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',
            'skill_filter': 'filipina_in_phl_no_av',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'Maids Line',
            'api_key_alias': 'OPENAI_KEY'  # Use default OpenAI key
        },
        # Task 39 — Prospect Nationality Service (Sales Nationality Identification)
        'Prospect_Nationality_Service': {
            'bot_skills': ['SALES_NATIONALITY_SERVICE_IDENTIFICATION'],
            'agent_skills': ['GPT CC Shadowers'],
            'table_name': 'SILVER.CHAT_EVALS.CC_SALES_CHATS',
            'skill_filter': 'sales_nationality_service_identification',
            'bot_filter': 'bot',
            'GPT_AGENT_NAME': 'CC Enchanters v2 Agent',
            'api_key_alias': 'OPENAI_KEY'
        }
    }


def get_llm_prompts_config():
    """
    LLM prompts configuration for each department.
    All departments use SA_PROMPT for sentiment analysis with NPS scoring.
    MV_Resolvers additionally keeps existing client_suspecting_ai prompts in JSON format.
    """
    # Common SA_prompt configuration for all departments
    sa_prompt_config = {
        'prompt': "",
        'system_prompt': SA_PROMPT,  # The detailed SA_PROMPT instructions become the system prompt
        'conversion_type': 'segment',
        'model_type': 'openai',
        'model': 'gpt-4o-mini',
        'temperature': 0,
        'max_tokens': 30000,
        'output_table': 'SA_RAW_DATA',  # Will be set per department below
        'reasoning_effort': ('low' if ('@Prompt@' in SA_PROMPT) else 'minimal'),
        'response_format': 'json_object'
    }

    client_suspecting_ai_prompt_config = {
        'prompt': "",
        'system_prompt': CLIENT_SUSPECTING_AI_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-nano',
        'temperature': 0.2,
        'max_tokens': 40000,
        'output_table': 'CLIENT_SUSPECTING_AI_RAW_DATA',
        'reasoning_effort': ('medium' if ('@Prompt@' in CLIENT_SUSPECTING_AI_PROMPT) else 'minimal')
        }

    legal_alignment_prompt_config = {
        'prompt': "",
        'system_prompt': LEGAL_ALIGNMENT_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-nano',
        'temperature': 0.2,
        'max_tokens': 40000,
        'output_table': 'LEGAL_ALIGNMENT_RAW_DATA',
        'reasoning_effort': 'medium',
        'response_format': 'json_object'
        }

    questioning_legalities_prompt_config = {
        'prompt': "",
        # Overridden per department (CC_Resolvers vs CC_Delighters)
        'system_prompt': "",
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.1,
        'max_tokens': 40000,
        'output_table': 'QUESTIONING_LEGALITIES_RAW_DATA',
        'reasoning_effort': 'low',
        # Token/correctness safeguard: exclude agent messages + stop at first agent intervention
        'exclude_agent_and_after_agent': True,
        'response_format': 'json_object'
    }

    call_request_prompt_config = {
        'prompt': "",
        'system_prompt': CALL_REQUEST_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-nano',
        'temperature': 0.2,
        'max_tokens': 40000,
        'output_table': 'CALL_REQUEST_RAW_DATA',
        'reasoning_effort': 'medium',
        'response_format': 'json_object'
        }

    categorizing_prompt_config = {
        'prompt': "",
        'system_prompt': CATEGORIZING_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.2,
        'max_tokens': 40000,
        'output_table': 'CATEGORIZING_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in CATEGORIZING_PROMPT) else 'minimal'),
        'response_format': 'json_object'
        }

    false_promises_prompt_config = {
        'prompt': "",
        'system_prompt': FALSE_PROMISES_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.2,
        'max_tokens': 40000,
        'output_table': 'FALSE_PROMISES_RAW_DATA',
        
        'reasoning_effort': ('low' if ('@Prompt@' in FALSE_PROMISES_PROMPT) else 'minimal'),
        'response_format': 'json_object'
        }

    # Task 29 — CC Delighters — Complexity Violation Rate %
    complexity_violation_prompt_config = {
        'prompt': "",
        # Overridden per department if needed
        'system_prompt': CC_DELIGHTERS_COMPLEXITY_VIOLATION_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.1,
        'max_tokens': 10000,
        'output_table': 'COMPLEXITY_VIOLATION_RAW_DATA',
        'reasoning_effort': 'low',
        # Token/correctness safeguard: exclude agent messages + stop at first agent intervention
        'exclude_agent_and_after_agent': True,
        'response_format': 'json_object'
    }

    ftr_prompt_config = {
        'prompt': "",
        'system_prompt': FTR_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.0,
        'max_tokens': 40000,
        'output_table': 'FTR_RAW_DATA',
        'reasoning_effort': 'medium',
        'response_format': 'json_object'
        }
    
    # AT_African Assessment and Document Analysis Configs
    incorrect_assessment_1_prompt_config = {
        'prompt': "",
        'system_prompt': AT_AFRICAN_INCORRECT_ASSESSMENT_1_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.0,
        'max_tokens': 5000,
        'output_table': 'INCORRECT_ASSESSMENT_1_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }
    
    incorrect_assessment_2_prompt_config = {
        'prompt': "",
        'system_prompt': AT_AFRICAN_INCORRECT_ASSESSMENT_2_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.0,
        'max_tokens': 5000,
        'output_table': 'INCORRECT_ASSESSMENT_2_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }
    
    poke_check_prompt_config = {
        'prompt': "",
        'system_prompt': AT_AFRICAN_POKE_CHECK_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.0,
        'max_tokens': 8000,
        'output_table': 'POKE_CHECK_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }
    
    facephoto_analysis_prompt_config = {
        'prompt': "",
        'system_prompt': AT_AFRICAN_FACEPHOTO_ANALYSIS_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.0,
        'max_tokens': 10000,
        'output_table': 'FACEPHOTO_ANALYSIS_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }
    
    passport_analysis_prompt_config = {
        'prompt': "",
        'system_prompt': AT_AFRICAN_PASSPORT_ANALYSIS_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.0,
        'max_tokens': 10000,
        'output_table': 'PASSPORT_ANALYSIS_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }
    
    mfa_analysis_prompt_config = {
        'prompt': "",
        'system_prompt': AT_AFRICAN_MFA_ANALYSIS_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.0,
        'max_tokens': 10000,
        'output_table': 'MFA_ANALYSIS_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }
    
    gcc_analysis_prompt_config = {
        'prompt': "",
        'system_prompt': AT_AFRICAN_GCC_ANALYSIS_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.0,
        'max_tokens': 10000,
        'output_table': 'GCC_ANALYSIS_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    doctors_categorizing_prompt_config = {
        'prompt': "",
        'system_prompt': DOCTORS_CATEGORIZING_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'gemini',
        'model': 'gemini-2.5-flash',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'DOCTORS_CATEGORIZING_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in DOCTORS_CATEGORIZING_PROMPT) else 'minimal'),
        'response_format': 'json_object'
    }

    legitimacy_prompt_config = {
        'prompt': "",
        'system_prompt': APPLICANTS_LEGITIMACY_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-4o',
        'temperature': 0.1,
        'max_tokens': 40000,
        'output_table': 'LEGITIMACY_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }


    dissatisfaction_prompt_config = {
        'prompt': "",
        'system_prompt': CUSTOMER_DISSATISFACTION_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-nano',
        'temperature': 0.0,
        'max_tokens': 40000,
        'output_table': 'DISSATISFACTION_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    cc_delighters_missed_tool_prompt_config = {
        'prompt': "",
        'system_prompt': CC_DELIGHTERS_MISSED_TOOL_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-nano',
        'temperature': 0.0,
        'max_tokens': 40000,
        'output_table': 'CC_DELIGHTERS_MISSED_TOOL_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    cc_resolvers_unclear_policy_prompt_config = {
        'prompt': "",
        'system_prompt': CC_RESOLVERS_UNCLEAR_POLICY_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.0,
        'max_tokens': 40000,
        'output_table': 'CC_RESOLVERS_UNCLEAR_POLICY_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }


    doctors_misprescription_prompt_config = {
        'prompt': "",
        'system_prompt': DOCTORS_MISPRESCRIPTION_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.2,
        'max_tokens': 40000,
        'output_table': 'DOCTORS_MISPRESCRIPTION_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    doctors_unnecessary_clinic_prompt_config = {
        'prompt': "",
        'system_prompt': DOCTORS_UNNECESSARY_CLINIC_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.2,
        'max_tokens': 40000,
        'output_table': 'DOCTORS_UNNECESSARY_CLINIC_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    clarity_score_prompt_config = {
        'prompt': "",
        'system_prompt': CLARITY_SCORE_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.2,
        'max_tokens': 10000,
        'output_table': 'CLARITY_SCORE_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in CLARITY_SCORE_PROMPT) else 'minimal'),
        'response_format': 'json_object'
    }

    policy_escalation_prompt_config = {
        'prompt': "",
        'system_prompt': POLICY_ESCALATION_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'POLICY_ESCALATION_RAW_DATA',
        'reasoning_effort': 'medium',
        'response_format': 'json_object'
    }

    threatening_prompt_config = {
        'prompt': "",
        'system_prompt': THREATINING_PROMPT,  # Default for clients
        'system_prompt_by_user_type': {
            'client': THREATINING_PROMPT,
            'maid': MAID_THREATINING_PROMPT
        },
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'THREATENING_RAW_DATA',
        'reasoning_effort': 'medium',
        'per_user_type_mode': True,  # Flag to indicate user-type-specific prompts
        'response_format': 'json_object'
    }

    # Base configuration for loss_interest (shared settings)
    loss_interest_base_config = {
        'prompt': "",
        'conversion_type': 'xmlcw',
        'model_type': 'gemini',
        'model': 'gemini-2.5-pro',
        'temperature': 0.2,
        'max_tokens': 40000,
        'output_table': 'LOSS_INTEREST_RAW_DATA',
        'reasoning_effort': 'minimal',
        'response_format': 'json_object'
    }

    # AT_Filipina_In_PHL specific loss_interest configuration
    loss_interest_phl_config = {
        **loss_interest_base_config,
        'system_prompt': {
            "Filipina_in_PHl_Pending_valid_visa": LOSS_INTEREST_ACTIVE_VISA_PROMPT,
            "Filipina_in_PHl_Pending_Passport": LOSS_INTEREST_PHL_PASSPORT_PROMPT,
            "Filipina_in_PHl_Pending_Facephoto": LOSS_INTEREST_PHL_PROFILE_PROMPT,
            "Filipina_in_PHl_Pending_OEC_From_Company": LOSS_INTEREST_OEC_PROMPT,
            "Filipina_in_PHl_Pending_OEC_From_Maid": LOSS_INTEREST_OEC_PROMPT
        },
        'skills_cw': {
            "Filipina_in_PHl_Pending_valid_visa": 3,
            "Filipina_in_PHl_Pending_Passport": 5,
            "Filipina_in_PHl_Pending_Facephoto": 5,
            "Filipina_in_PHl_Pending_OEC_From_Company": 8,
            "Filipina_in_PHl_Pending_OEC_From_Maid": 8
        },
        'category_mapping': {
            'Active Visa Submission Philippines': ['FILIPINA_IN_PHL_PENDING_VALID_VISA'],
            'Passport Submission (Photo) Philippines': ['FILIPINA_IN_PHL_PENDING_PASSPORT'],
            'Photo Submission (Photo) Philippines': ['FILIPINA_IN_PHL_PENDING_FACEPHOTO'],
            'OEC Philippines': ['FILIPINA_IN_PHL_PENDING_OEC_FROM_MAID', 'FILIPINA_IN_PHL_PENDING_OEC_FROM_COMPANY']
        }
    }

    # AT_Filipina_Outside_UAE specific loss_interest configuration
    loss_interest_outside_config = {
        **loss_interest_base_config,
        'system_prompt': {
            "Filipina_Outside_Pending_Facephoto": LOSS_INTEREST_OUTSIDE_PROFILE_PROMPT,
            "Filipina_Outside_Pending_Passport": LOSS_INTEREST_OUTSIDE_PASSPORT_PROMPT,
            "Filipina_Outside_UAE_Pending_Joining_Date": LOSS_INTEREST_EXPECTED_DTJ_PROMPT
        },
        'skills_cw': {
            "Filipina_Outside_Pending_Facephoto": 5,
            "Filipina_Outside_Pending_Passport": 5,
            "Filipina_Outside_UAE_Pending_Joining_Date": 8
        },
        'category_mapping': {
            'Photo Submission Outside UAE': ['FILIPINA_OUTSIDE_PENDING_FACEPHOTO'],
            'Passport Submission Outside UAE': ['FILIPINA_OUTSIDE_PENDING_PASSPORT'],
            'Joining Date Outside UAE': ['FILIPINA_OUTSIDE_UAE_PENDING_JOINING_DATE']
        }
    }

    loss_interest_at_filipina_config = {
        **loss_interest_base_config,
        'system_prompt': {
            "Filipina_in_PHl_Pending_valid_visa": LOSS_INTEREST_ACTIVE_VISA_PROMPT,
            "Filipina_in_PHl_Pending_Passport": LOSS_INTEREST_PHL_PASSPORT_PROMPT,
            "Filipina_in_PHl_Pending_Facephoto": LOSS_INTEREST_PHL_PROFILE_PROMPT,
            "Filipina_in_PHl_Pending_OEC_From_Company": LOSS_INTEREST_OEC_PROMPT,
            "Filipina_in_PHl_Pending_OEC_From_Maid": LOSS_INTEREST_OEC_PROMPT,
            "Filipina_Outside_Pending_Facephoto": LOSS_INTEREST_OUTSIDE_PROFILE_PROMPT,
            "Filipina_Outside_Pending_Passport": LOSS_INTEREST_OUTSIDE_PASSPORT_PROMPT,
            "Filipina_Outside_UAE_Pending_Joining_Date": LOSS_INTEREST_EXPECTED_DTJ_PROMPT
        },
        'skills_cw': {
            "Filipina_in_PHl_Pending_valid_visa": 3,
            "Filipina_in_PHl_Pending_Passport": 5,
            "Filipina_in_PHl_Pending_Facephoto": 5,
            "Filipina_in_PHl_Pending_OEC_From_Company": 8,
            "Filipina_in_PHl_Pending_OEC_From_Maid": 8,
            "Filipina_Outside_Pending_Facephoto": 5,
            "Filipina_Outside_Pending_Passport": 5,
            "Filipina_Outside_UAE_Pending_Joining_Date": 8
        },
        'category_mapping': {
            'Active Visa Submission Philippines': ['FILIPINA_IN_PHL_PENDING_VALID_VISA'],
            'Passport Submission (Photo) Philippines': ['FILIPINA_IN_PHL_PENDING_PASSPORT'],
            'Photo Submission (Photo) Philippines': ['FILIPINA_IN_PHL_PENDING_FACEPHOTO'],
            'OEC Philippines': ['FILIPINA_IN_PHL_PENDING_OEC_FROM_MAID', 'FILIPINA_IN_PHL_PENDING_OEC_FROM_COMPANY'],
            'Photo Submission Outside UAE': ['FILIPINA_OUTSIDE_PENDING_FACEPHOTO'],
            'Passport Submission Outside UAE': ['FILIPINA_OUTSIDE_PENDING_PASSPORT'],
            'Joining Date Outside UAE': ['FILIPINA_OUTSIDE_UAE_PENDING_JOINING_DATE']
        }
    }

    intervention_prompt_config = {
        'prompt': "",
        'system_prompt': INTERVENTION_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0,
        'max_tokens': 40000,
        'output_table': 'CATEGORIZING_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in INTERVENTION_PROMPT) else 'minimal'),
        'response_format': 'json_object'
    }

    clinic_recommendation_reason_prompt_config = {
        'prompt': "",
        'system_prompt': CLINIC_RECOMMENDATION_REASON_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'gemini',
        'model': 'gemini-2.5-pro',
        'temperature': 0.2,
        'max_tokens': 7000,
        'output_table': 'CLINIC_RECOMMENDATION_REASON_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in CLINIC_RECOMMENDATION_REASON_PROMPT) else 'minimal'),
        'response_format': 'json_object'
    }

    missing_policy_prompt_config = {
        'prompt': "",
        'system_prompt': MISSING_POLICY_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0,
        'max_tokens': 30000,
        'output_table': 'MISSING_POLICY_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in MISSING_POLICY_PROMPT) else 'minimal'),
        'response_format': 'json_object'
        }

    unclear_policy_prompt_config = {
        'prompt': "",
        'system_prompt': UNCLEAR_POLICY_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0,
        'max_tokens': 30000,
        'output_table': 'UNCLEAR_POLICY_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in UNCLEAR_POLICY_PROMPT) else 'minimal'),
        'response_format': 'json_object'
        }

    policy_violation_prompt_config = {
        'prompt': "",
        'system_prompt': AT_FILIPINA_POLICY_VIOLATION_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 1,
        'max_tokens': 40000,
        'output_table': 'POLICY_VIOLATION_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in AT_FILIPINA_POLICY_VIOLATION_PROMPT) else 'minimal'),
        'response_format': 'json_object'
    }

    at_african_policy_violation_prompt_config = {
        'prompt': "",
        'system_prompt': AT_AFRICAN_POLICY_PORMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.1,
        'max_tokens': 40000,
        'output_table': 'POLICY_VIOLATION_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in AT_AFRICAN_POLICY_PORMPT) else 'minimal'),
        'response_format': 'json_object'
    }

    sales_transfer_prompt_config = {
        'prompt': "",
        'system_prompt': CC_SALES_TRANSFER_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'TRANSFER_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in CC_SALES_TRANSFER_PROMPT) else 'minimal'),
        'response_format': 'json_object'
    }

    doctors_transfer_escalation_prompt_config = {
        'prompt': "",
        'system_prompt': DOCTORS_TRANSFER_ESCALATION_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'TRANSFER_ESCALATION_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    doctors_transfer_known_flow_prompt_config = {
        'prompt': "",
        'system_prompt': DOCTORS_TRANSFER_KNOWN_FLOW_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.0,
        'max_tokens': 30000,
        'output_table': 'TRANSFER_KNOWN_FLOW_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    doctors_insurance_complaints_prompt_config = {
        'prompt': "",
        'system_prompt': DOCTORS_INSURANCE_COMPLAINTS_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.0,
        'max_tokens': 30000,
        'output_table': 'INSURANCE_COMPLAINTS_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    doctors_document_request_prompt_config = {
        'prompt': "",
        'system_prompt': DOCTORS_DOCUMENT_REQUEST_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.0,
        'max_tokens': 40000,
        'output_table': 'DOCUMENT_REQUEST_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    doctors_wrong_tool_prompt_config = {
        'prompt': "",
        'system_prompt': DOCTORS_WRONG_TOOL_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.0,
        'max_tokens': 40000,
        'output_table': 'WRONG_TOOL_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    doctors_missing_tool_prompt_config = {
        'prompt': "",
        'system_prompt': DOCTORS_MISSING_TOOL_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.0,
        'max_tokens': 40000,
        'output_table': 'MISSING_TOOL_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    doctors_health_check_prompt_config = {
        'prompt': "",
        'system_prompt': DOCTORS_HEALTH_CHECK_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.0,
        'max_tokens': 40000,
        'output_table': 'HEALTH_CHECK_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    mv_resolvers_wrong_tool_prompt_config = {
        'prompt': "",
        'system_prompt': MV_RESOLVERS_WRONG_TOOL_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'WRONG_TOOL_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in MV_RESOLVERS_WRONG_TOOL_PROMPT) else 'minimal'),
        'response_format': 'json_object'
        }

    mv_resolvers_missing_tool_prompt_config = {
        'prompt': "",
        'system_prompt': MV_RESOLVERS_MISSING_TOOL_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'MISSING_TOOL_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in MV_RESOLVERS_MISSING_TOOL_PROMPT) else 'minimal'),
        'response_format': 'json_object'
        }

    tool_prompt_config = {
        'prompt': "",
        'system_prompt': DOCTORS_TOOL_PROMPT,
        'conversion_type': 'json',
        'model_type': 'gemini',
        'model': 'gemini-2.5-pro',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'TOOL_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in DOCTORS_TOOL_PROMPT) else 'minimal'),
        'response_format': 'json_object'
    }

    cc_sales_policy_violation_prompt_config = {
        'prompt': "",
        'system_prompt': CC_SALES_POLICY_VIOLATION_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.2,
        'max_tokens': 40000,
        'output_table': 'POLICY_VIOLATION_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in CC_SALES_POLICY_VIOLATION_PROMPT) else 'minimal'),
        'response_format': 'json_object'
    }

    test_prompt_config = {
        'prompt': "",
        'system_prompt': TEST_PROMPT,
        'conversion_type': 'json_private',
        'model_type': 'gemini',
        'model': 'gemini-2.5-pro',
        'temperature': -1,
        'max_tokens': 0,
        'output_table': 'TEST_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in TEST_PROMPT) else 'minimal'),
        'response_format': 'json_object'
    }

    transfer_prompt_config = {
        'prompt': "",
        'system_prompt': AT_AFRICAN_TRANSFER_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-4o',
        'temperature': 0.1,
        'max_tokens': 40000,
        'output_table': 'TRANSFER_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in AT_AFRICAN_TRANSFER_PROMPT) else 'minimal'),
        'response_format': 'json_object'
    }
    
    kenyan_flow_order_prompt_config = {
        'prompt': "",
        'system_prompt': KENYAN_FLOW_ORDER_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'FLOW_ORDER_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }
    
    kenyan_profile_update_prompt_config = {
        'prompt': "",
        'system_prompt': KENYAN_PROFILE_UPDATE_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'PROFILE_UPDATE_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }
    
    # Filipina configs - reusing the same Kenyan prompts with Filipina-specific ERP variables
    filipina_flow_order_prompt_config = {
        'prompt': "",
        'system_prompt': KENYAN_FLOW_ORDER_PROMPT,  # Reusing same prompt
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'FLOW_ORDER_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }
    
    filipina_profile_update_prompt_config = {
        'prompt': "",
        'system_prompt': KENYAN_PROFILE_UPDATE_PROMPT,  # Reusing same prompt
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'PROFILE_UPDATE_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }
    
    exceptions_granted_prompt_config = {
        'prompt': "",
        'system_prompt': EXCEPTIONS_GRANTED_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0,
        'max_tokens': 40000,
        'output_table': 'EXCEPTIONS_GRANTED_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    cc_resolvers_categorizing_prompt_config = {
        'prompt': "",
        'system_prompt': CC_RESOLVERS_CATEGORIZING_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.1,
        'max_tokens': 40000,
        'output_table': 'CATEGORIZING_RAW_DATA',
        'prompt_type': 'cc_resolvers_categorizing',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    cc_resolvers_policy_escalation_prompt_config = {
        'prompt': "",
        'system_prompt': CC_RESOLVERS_POLICY_ESCALATION_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.1,
        'max_tokens': 50000,
        'output_table': 'POLICY_ESCALATION_RAW_DATA',
        'prompt_type': 'cc_resolvers_policy_escalation',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    cc_delighters_categorizing_prompt_config = {
        'prompt': "",
        'system_prompt': CC_DELIGHTERS_CATEGORIZING_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.1,
        'max_tokens': 40000,
        'output_table': 'CATEGORIZING_RAW_DATA',
        'prompt_type': 'cc_delighters_categorizing',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    cc_delighters_policy_escalation_prompt_config = {
        'prompt': "",
        'system_prompt': CC_DELIGHTERS_POLICY_ESCALATION_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.1,
        'max_tokens': 50000,
        'output_table': 'POLICY_ESCALATION_RAW_DATA',
        'prompt_type': 'cc_delighters_policy_escalation',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    cc_sales_agent_intervention_prompt_config = {
        'prompt': "",
        'system_prompt': CC_SALES_AGENT_INTERVENTION_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0,
        'max_tokens': 30000,
        'output_table': 'AGENT_INTERVENTION_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }


    grammar_prompt_config = {
        'prompt': "",
        'system_prompt': GRAMMAR_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-4o',
        'temperature': 0.1,
        'max_tokens': 1500,
        'output_table': 'GRAMMAR_RAW_DATA',
        'reasoning_effort': ('low' if ('@Prompt@' in GRAMMAR_PROMPT) else 'minimal'),
        'response_format': 'json_object'
    }

    request_retraction_prompt_config = {
        'prompt': "",
        'system_prompt': CC_RESOLVERS_REQUEST_RETRACTION_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.1,
        'max_tokens': 10000,
        'output_table': 'REQUEST_RETRACTION_RAW_DATA',
        'reasoning_effort': 'low',
        'prompt_type': 'request_retraction',
        'response_format': 'json_object'
    }

    # Task 38 — CC Resolvers — Cancellation Request Reasons & Retraction (similar to Task 32 for Replacement)
    cancellation_retraction_prompt_config = {
        'prompt': "",
        'system_prompt': CC_RESOLVERS_CANCELLATION_RETRACTION_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.0,
        'max_tokens': 30000,
        'output_table': 'CANCELLATION_RETRACTION_RAW_DATA',
        'reasoning_effort': 'low',
        'prompt_type': 'cancellation_retraction',
        'response_format': 'json_object'
    }

    # Task 33/34 — Document Request Failure % (CC_Resolvers + CC_Delighters)
    document_request_failure_prompt_config = {
        'prompt': "",
        # Overridden per department
        'system_prompt': "",
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.0,
        'max_tokens': 30000,
        'output_table': 'DOCUMENT_REQUEST_RAW_DATA',
        'reasoning_effort': 'low',
        'prompt_type': 'document_request_failure',
        # Token/correctness safeguard: exclude agent messages + stop at first agent intervention
        'exclude_agent_and_after_agent': True,
    }

    cc_resolvers_policy_transfer_prompt_config = {
        'prompt': "",
        'system_prompt': CC_RESOLVERS_POLICY_TRANSFER_PROMPT,
        'conversion_type': 'json_private',
        'model_type': 'openai',
        'model': 'gpt-4.1',
        'temperature': 0,
        'max_tokens': 30000,
        'output_table': 'POLICY_TRANSFER_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    client_mention_another_maid_prompt_config = {
        'prompt': "",
        'system_prompt': CLIENT_MENTION_ANOTHER_MAID_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-nano',
        'temperature': -1,
        'max_tokens': 30000,
        'output_table': 'CLIENT_MENTION_ANOTHER_MAID_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    promise_no_tool_triggered_prompt_config = {
        'prompt': "",
        'system_prompt': PROMISE_NO_TOOL_TRIGGERED_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0,
        'max_tokens': 30000,
        'output_table': 'PROMISE_NO_TOOL_TRIGGERED_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    # AT African Negative Tool Response Configuration
    at_african_negative_tool_response_config = {
        'prompt': "",
        'system_prompt': AT_AFRICAN_NEGATIVE_TOOL_RESPONSE_PROMPT,
        'model': 'gpt-5-mini',
        'model_type': 'openai',
        'conversion_type': 'json',
        'temperature': 0.1,
        'max_tokens': 1000,
        'output_table': 'NEGATIVE_TOOL_RESPONSE_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    # New Repetition Configuration (Exact + Contextual)
    new_repetition_prompt_config = {
        'prompt': "",
        'system_prompt': NEW_REPETITION_PROMPT,
        'model': 'gpt-5-mini',
        'model_type': 'openai',
        'conversion_type': 'json',
        'temperature': 0.1,
        'max_tokens': 1500,
        'output_table': 'NEW_REPETITION_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    backed_out_prompt_config = {
        'prompt': "",
        'system_prompt': BACKED_OUT_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.1,
        'max_tokens': 5000,
        'output_table': 'BACKED_OUT_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    closing_message_prompt_config = {
        'prompt': "",
        'system_prompt': CLOSING_MESSAGE_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0.1,
        'max_tokens': 1500,
        'output_table': 'CLOSING_MESSAGE_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    clarification_prompt_config = {
        'prompt': "",
        'system_prompt': CC_RESOLVERS_CLARIFICATION_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0,
        'max_tokens': 30000,
        'output_table': 'CLARIFICATION_RAW_DATA',
        'reasoning_effort': 'minimal',
        'response_format': 'json_object'
    }

    sales_wrong_answer_prompt_config = {
        'prompt': "",
        'system_prompt': MV_SALES_WRONG_ANSWER_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-nano',
        'temperature': 0,
        'max_tokens': 30000,
        'output_table': 'WRONG_ANSWER_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    sales_unsatisfactory_policy_prompt_config = {
        'prompt': "",
        'system_prompt': MV_SALES_UNSATISFACTORY_POLICY_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-nano',
        'temperature': 0,
        'max_tokens': 30000,
        'output_table': 'UNSATISFACTORY_POLICY_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    repetition_prompt_config = {
        'prompt': "",
        'system_prompt': FILIPINA_REPETITION_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-4o',
        'temperature': 0.1,
        'max_tokens': 1500,
        'output_table': 'FILIPINA_REPETITION_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    cc_delighters_wrong_tool_prompt_config = {
        'prompt': "",
        'system_prompt': CC_DELIGHTERS_WRONG_TOOL_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.0,
        'max_tokens': 30000,
        'output_table': 'WRONG_TOOL_RAW_DATA',
        'prompt_type': 'wrong_tool',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    de_escalation_prompt_config = {
        'prompt': "",
        'system_prompt': DE_ESCALATION_SUCCESS_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.1,
        'max_tokens': 1500,
        'output_table': 'DE_ESCALATION_RAW_DATA',
        'prompt_type': 'de_escalation',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    cc_delighters_unclear_policy_prompt_config = {
        'prompt': "",
        'system_prompt': CC_DELIGHTERS_UNCLEAR_POLICY_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.0,
        'max_tokens': 1500,
        'output_table': 'UNCLEAR_POLICY_RAW_DATA',
        'prompt_type': 'unclear_policy',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    # threatening_case_identifier_prompt_config = {
    #     'prompt': "",
    #     'system_prompt': THREATENING_CASE_IDENTIFIER_PROMPT,
    #     'conversion_type': 'json_private',
    #     'model_type': 'openai',
    #     'model': 'gpt-5-nano',
    #     'temperature': 0.0,
    #     'max_tokens': 30000,
    #     'output_table': 'THREATENING_CASE_IDENTIFIER_RAW_DATA',
    #     'prompt_type': 'threatening_case_identifier',
    #     'reasoning_effort': 'low'
    # }


    # test_prompt_config = {
    #     'prompt': "",
    #     'system_prompt': TEST_MERGED_PROMPT,
    #     'conversion_type': 'xml',
    #     'model_type': 'openai',
    #     'model': 'gpt-5',
    #     'temperature': 0,
    #     'max_tokens': 30000,
    #     'output_table': 'TEST_RAW_DATA',
    #     'reasoning_effort': 'low'
    # }

    ignored_client_prompt_config = {
        'prompt': "",
        'system_prompt': MV_RESOLVERS_IGNORED_CLIENT_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5-mini',
        'temperature': 0,
        'max_tokens': 30000,
        'output_table': 'IGNORED_CLIENT_RAW_DATA',
        'prompt_type': 'ignored_client',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    prompt_compliance_prompt_config = {
        'prompt': "",
        'system_prompt': MV_RESOLVERS_PROMPT_COMPLIANCE_PROMPT,
        'conversion_type': 'xml',
        'model_type': 'openai',
        'model': 'gpt-5.1',
        'temperature': 0,
        'max_tokens': 30000,
        'output_table': 'PROMPT_COMPLIANCE_RAW_DATA',
        'prompt_type': 'prompt_compliance',
        'reasoning_effort': 'medium',
        'response_format': 'json_object'
    }

    tool_eval_prompt_config = {
        'prompt': "",
        'system_prompt': CC_SALES_TOOL_EVAL_PROMPT,
        'conversion_type': 'message_segment',
        'model_type': 'openai',
        'model': 'gpt-5.1',
        'temperature': 0,
        'max_tokens': 30000,
        'output_table': 'TOOL_EVAL_RAW_DATA',
        'reasoning_effort': 'low',
        'dynamic_prompt_udf': 'BUILD_TOOL_EVAL_SYSTEM_PROMPT_MV_SALES'  # UDF for dynamic prompt generation
    }
    
    shadowing_automation_prompt_config = {
        'prompt': "",
        'system_prompt': UNIQUE_ISSUES_PROMPT,
        'conversion_type': 'text',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.2,
        'max_tokens': 30000,
        'output_table': 'UNIQUE_ISSUES_RAW_DATA',
        'reasoning_effort': 'low',
        'response_format': 'json_object'
    }

    doctor_agent_intervention_prompt_config = {
        'prompt': "",
        'system_prompt': DOCTOR_AGENT_INTERVENTION_PROMPT,
        'conversion_type': 'json',
        'model_type': 'openai',
        'model': 'gpt-5',
        'temperature': 0.2,
        'max_tokens': 40000,
        'output_table': 'DOCTOR_AGENT_INTERVENTION_RAW_DATA',
        'reasoning_effort': 'high',
        'response_format': 'json_object'
    }

    return {
        'Routing_Bot': {
            # 'SA_prompt': {**sa_prompt_config, 'model': 'gpt-5', 'reasoning_effort': 'minimal'},
            'call_request': {**call_request_prompt_config, 'system_prompt': ROUTING_BOT_CALL_REQUESTS_PROMPT, 'model_type': 'openai', 'model': 'gpt-5-nano', 'conversion_type': 'xml', 'temperature': 0, 'reasoning_effort': 'low'},
            'tool': {**tool_prompt_config, 'system_prompt': ROUTING_BOT_TOOL_PROMPT, 'model_type': 'openai', 'model': 'gpt-5-nano', 'conversion_type': 'xml', 'temperature': 0, 'reasoning_effort': 'low'},
            'false_promises': {**false_promises_prompt_config, 'system_prompt': ROUTING_BOT_FALSE_PROMISES_PROMPT, 'model_type': 'openai', 'model': 'gpt-5', 'conversion_type': 'xml', 'temperature': 0, 'reasoning_effort': 'minimal'},
            'threatening': threatening_prompt_config,
        },
        'CC_Resolvers': {
            # 'SA_prompt': sa_prompt_config,
            "test": test_prompt_config,
            'call_request': {**call_request_prompt_config, 'system_prompt': CC_RESOLVERS_CALL_REQUEST_PROMPT, 'model': 'gpt-5-nano', 'conversion_type': 'json', 'temperature': 0.0, 'max_tokens': 40000, 'reasoning_effort': 'low', 'exclude_agent_and_after_agent': True},
            'client_suspecting_ai': {**client_suspecting_ai_prompt_config, 'system_prompt': CC_RESOLVERS_CLIENT_SUSPECTING_AI_PROMPT, 'model': 'gpt-5', 'conversion_type': 'json', 'temperature': 0, 'reasoning_effort': 'minimal', 'exclude_agent_and_after_agent': True},
            'questioning_legalities': {**questioning_legalities_prompt_config, 'system_prompt': CC_RESOLVERS_CLIENTS_QUESTIONING_LEGALITIES_PROMPT},
            # Task 30 — CC Resolvers — False Promises %
            'false_promises': {**false_promises_prompt_config, 'system_prompt': CC_RESOLVERS_FALSE_PROMISES_PROMPT, 'conversion_type': 'xml', 'model': 'gpt-5', 'temperature': 0.1, 'max_tokens': 10000, 'reasoning_effort': 'low', 'exclude_agent_and_after_agent': True},
            # Task 35 — CC Resolvers — Wrong Answer %
            'wrong_answer': {**sales_wrong_answer_prompt_config, 'system_prompt': CC_RESOLVERS_WRONG_ANSWER_PROMPT, 'model': 'gpt-5', 'conversion_type': 'xml', 'temperature': 0.0, 'max_tokens': 30000, 'reasoning_effort': 'low', 'exclude_agent_and_after_agent': True},
            'categorizing': cc_resolvers_categorizing_prompt_config,
            # 'policy_escalation': cc_resolvers_policy_escalation_prompt_config,
            # 'exceptions_granted': {**exceptions_granted_prompt_config, 'exclude_agent_and_after_agent': True},
            'policy_transfer': cc_resolvers_policy_transfer_prompt_config,
            'threatening': {**threatening_prompt_config, 'exclude_agent_and_after_agent': True},
            'promise_no_tool_triggered': promise_no_tool_triggered_prompt_config,
            # 'clarification': clarification_prompt_config,
            'request_retraction': {**request_retraction_prompt_config, 'exclude_agent_and_after_agent': True},
            # Task 38 — CC Resolvers — Cancellation Request Reasons & Retraction
            'cancellation_retraction': {**cancellation_retraction_prompt_config, 'exclude_agent_and_after_agent': True},
            'document_request_failure': {**document_request_failure_prompt_config, 'system_prompt': CC_RESOLVERS_DOCUMENT_REQUEST_FAILURE_PROMPT, 'exclude_agent_and_after_agent': True},
            "missing_policy": {**missing_policy_prompt_config, 'system_prompt': CC_RESOLVERS_MISSING_POLICY_PROMPT, 'model': 'gpt-5', 'conversion_type': 'json', 'temperature': 0, 'max_tokens': 5000, 'reasoning_effort': ('low' if ('@Prompt@' in CC_RESOLVERS_MISSING_POLICY_PROMPT) else 'minimal'), 'exclude_agent_and_after_agent': True},
            "wrong_tool": {**mv_resolvers_wrong_tool_prompt_config, 'system_prompt': CC_RESOLVERS_WRONG_TOOL_PROMPT, 'model': 'gpt-5-nano', 'conversion_type': 'json_private', 'temperature': 0.0, 'max_tokens': 40000, 'reasoning_effort': 'low', 'exclude_agent_and_after_agent': True},
            "missing_tool": {**mv_resolvers_missing_tool_prompt_config, 'system_prompt': CC_RESOLVERS_MISSING_TOOL_PROMPT, 'model': 'gpt-5-nano', 'conversion_type': 'json_private', 'temperature': 0.0, 'max_tokens': 40000, 'reasoning_effort': 'low', 'exclude_agent_and_after_agent': True},
            'unclear_policy': {**cc_resolvers_unclear_policy_prompt_config, 'exclude_agent_and_after_agent': True},
            # 'shadowing_automation': {**shadowing_automation_prompt_config, 'system_prompt': UNIQUE_ISSUES_PROMPT_CC_RESOLVERS},
            "ftr": {**ftr_prompt_config, 'system_prompt': FTR_CC_RESOLVERS_PROMPT, 'output_table': 'FTR_CC_RESOLVERS_RAW_DATA'},
        },
        'MV_Resolvers': {
            # 'SA_prompt': {**sa_prompt_config, 'model': 'gpt-5', 'conversion_type': 'xml', 'reasoning_effort': 'minimal'},
            'client_suspecting_ai': client_suspecting_ai_prompt_config,
            'legal_alignment': legal_alignment_prompt_config,
            'call_request': call_request_prompt_config,
            'categorizing': categorizing_prompt_config,
            'false_promises': false_promises_prompt_config,
            "ftr": ftr_prompt_config,
            # "threatening": threatening_prompt_config,
            # "policy_escalation": policy_escalation_prompt_config,
            # "clarity_score": clarity_score_prompt_config,
            "missing_policy": missing_policy_prompt_config,
            "unclear_policy": unclear_policy_prompt_config,
            # "mv_resolvers_wrong_tool": mv_resolvers_wrong_tool_prompt_config,
            # "mv_resolvers_missing_tool": mv_resolvers_missing_tool_prompt_config,
            # "ignored_client": ignored_client_prompt_config,
            # "prompt_compliance": prompt_compliance_prompt_config,
            # "client_mention_another_maid": client_mention_another_maid_prompt_config,
            # "test": test_prompt_config
            
            # Tool Eval for MV_Resolvers (static prompt with @Prompt@ replacement)
            'tool_eval': {
                'prompt': "",
                'system_prompt': MV_RESOLVERS_TOOL_EVAL_PROMPT,
                'conversion_type': 'message_segment',
                'model_type': 'openai',
                'model': 'gpt-5',
                'temperature': 0,
                'max_tokens': 30000,
                'output_table': 'TOOL_EVAL_RAW_DATA',
                'reasoning_effort': 'low',
                # NO dynamic_prompt_udf - triggers MV_Resolvers static prompt mode
            }
        },
        'Doctors': {
            # 'SA_prompt': sa_prompt_config,
            # 'doctors_categorizing': doctors_categorizing_prompt_config,
            'misprescription': doctors_misprescription_prompt_config,
            'unnecessary_clinic': doctors_unnecessary_clinic_prompt_config,
            # Override model to gemini-2.5-flash for these three prompt types only for Doctors
            # 'clarity_score': { **clarity_score_prompt_config, 'model': 'gemini-2.5-flash', 'model_type': 'gemini' },
            # 'policy_escalation': { **policy_escalation_prompt_config, 'model': 'gemini-2.5-flash', 'model_type': 'gemini' },
            'client_suspecting_ai': { **client_suspecting_ai_prompt_config, 'model': 'gemini-2.5-flash', 'model_type': 'gemini' },
            'intervention': intervention_prompt_config,
            # 'clinic_recommendation_reason': clinic_recommendation_reason_prompt_config,
            # 'tool': tool_prompt_config,
            'transfer_escalation': doctors_transfer_escalation_prompt_config,
            'transfer_known_flow': doctors_transfer_known_flow_prompt_config,
            'insurance_complaints': doctors_insurance_complaints_prompt_config,
            'missing_policy': {**missing_policy_prompt_config, 
                'system_prompt': DOCTORS_MISSING_POLICY_PROMPT,
                'model': 'gpt-5',
                'conversion_type': 'json',
                'temperature': 0.2,
                'max_tokens': 30000,
                'output_table': 'MISSING_POLICY_RAW_DATA',
                'reasoning_effort': 'low'
            },
            'threatening': {**threatening_prompt_config, 'system_prompt': THREATENING_CASE_IDENTIFIER_PROMPT, 'model': 'gpt-5-mini', 'conversion_type': 'json', 'output_table': 'THREATENING_CASE_IDENTIFIER_RAW_DATA', 'temperature': 0.0, 'max_tokens': 40000, 'reasoning_effort': 'low', 'per_user_type_mode': False},
            'doctor_agent_intervention': doctor_agent_intervention_prompt_config,
            'document_request': doctors_document_request_prompt_config,
            'wrong_tool': doctors_wrong_tool_prompt_config,
            'missing_tool': doctors_missing_tool_prompt_config,
            'health_check': doctors_health_check_prompt_config,
            # Task 37 — Doctors — Wrong Answer %
            # 'wrong_answer': {**sales_wrong_answer_prompt_config, 'system_prompt': DOCTORS_WRONG_ANSWER_PROMPT, 'model': 'gpt-5', 'conversion_type': 'xml', 'temperature': 0.0, 'max_tokens': 30000, 'reasoning_effort': 'low', 'exclude_agent_and_after_agent': True},
            "ftr": {**ftr_prompt_config, 'system_prompt': FTR_DOCTORS_PROMPT, 'output_table': 'FTR_DOCTORS_RAW_DATA'},
        },
        'MV_Delighters': {
            # 'SA_prompt': sa_prompt_config,
            'threatening': threatening_prompt_config,
            'client_suspecting_ai': client_suspecting_ai_prompt_config,
            'legal_alignment': legal_alignment_prompt_config,
            'call_request': call_request_prompt_config,
            'categorizing': categorizing_prompt_config,
            'false_promises': false_promises_prompt_config,
            "ftr": ftr_prompt_config,
            "policy_escalation": policy_escalation_prompt_config,
            # "clarity_score": clarity_score_prompt_config,
            "missing_policy": missing_policy_prompt_config,
            "unclear_policy": unclear_policy_prompt_config,
            # "mv_resolvers_wrong_tool": mv_resolvers_wrong_tool_prompt_config,
            # "mv_resolvers_missing_tool": mv_resolvers_missing_tool_prompt_config,
        },
        'CC_Delighters': {
            # 'SA_prompt': sa_prompt_config,
            "test": test_prompt_config,
            'missed_tool': cc_delighters_missed_tool_prompt_config,
            'call_request': {**call_request_prompt_config, 'system_prompt': CC_DELIGHTERS_CALL_REQUEST_PROMPT, 'model': 'gpt-5', 'conversion_type': 'json', 'temperature': 0.1, 'max_tokens': 10000, 'reasoning_effort': 'low', 'exclude_agent_and_after_agent': True},
            'questioning_legalities': {**questioning_legalities_prompt_config, 'system_prompt': CC_DELIGHTERS_MAIDS_QUESTIONING_LEGALITIES_PROMPT},
            'categorizing': cc_delighters_categorizing_prompt_config,
            # 'policy_escalation': cc_delighters_policy_escalation_prompt_config,
            'missing_policy': {**missing_policy_prompt_config, 'system_prompt': CC_DELIGHTERS_MISSING_POLICY_PROMPT, 'model': 'gpt-5', 'conversion_type': 'json', 'temperature': 0, 'max_tokens': 5000, 'reasoning_effort': ('low' if ('@Prompt@' in CC_DELIGHTERS_MISSING_POLICY_PROMPT) else 'minimal')},
            'wrong_tool': cc_delighters_wrong_tool_prompt_config,
            'unclear_policy': cc_delighters_unclear_policy_prompt_config,
            'threatening': threatening_prompt_config,
            "ftr": {**ftr_prompt_config, 'system_prompt': FTR_CC_DELIGHTERS_PROMPT, 'output_table': 'FTR_CC_DELIGHTERS_RAW_DATA'},
            # 'threatening_case_identifier': threatening_case_identifier_prompt_config,
            # 'shadowing_automation': {**shadowing_automation_prompt_config, 'system_prompt': UNIQUE_ISSUES_PROMPT_CC_DELIGHTERS},
            'false_promises': {**false_promises_prompt_config, 'system_prompt': CC_DELIGHTERS_FALSE_PROMISES_PROMPT, 'conversion_type': 'xml', 'model': 'gpt-5', 'temperature': 0.1, 'max_tokens': 10000, 'reasoning_effort': 'low', 'exclude_agent_and_after_agent': True},
            # Task 36 — CC Delighters — Wrong Answer %
            'wrong_answer': {**sales_wrong_answer_prompt_config, 'system_prompt': CC_DELIGHTERS_WRONG_ANSWER_PROMPT, 'model': 'gpt-5', 'conversion_type': 'xml', 'temperature': 0.0, 'max_tokens': 30000, 'reasoning_effort': 'low', 'exclude_agent_and_after_agent': True},
            'complexity_violation': {**complexity_violation_prompt_config, 'system_prompt': CC_DELIGHTERS_COMPLEXITY_VIOLATION_PROMPT, 'conversion_type': 'xml', 'model': 'gpt-5', 'temperature': 0.1, 'max_tokens': 10000, 'reasoning_effort': 'low', 'exclude_agent_and_after_agent': True},
            'client_suspecting_ai': {**client_suspecting_ai_prompt_config, 'system_prompt': CC_DELIGHTERS_MAID_SUSPECTING_AI_PROMPT, 'model': 'gpt-5', 'conversion_type': 'json', 'temperature': 0, 'reasoning_effort': 'minimal', 'exclude_agent_and_after_agent': True},
            'document_request_failure': {**document_request_failure_prompt_config, 'system_prompt': CC_DELIGHTERS_DOCUMENT_REQUEST_FAILURE_PROMPT, 'exclude_agent_and_after_agent': True},
            

        },
        'AT_Filipina': {
            # 'loss_interest': loss_interest_at_filipina_config,
            # Parent department - no prompts, only used for grouping subcategories
        },
        'AT_Filipina_In_PHL': {
            # 'SA_prompt': sa_prompt_config,
            'loss_interest': loss_interest_phl_config,
            'policy_violation': policy_violation_prompt_config,
            # 'tool': {**tool_prompt_config, 'system_prompt': AT_AFRICAN_TOOL_PROMPT, 'conversion_type': 'json', 'model_type': 'openai', 'model': 'gpt-5-mini', 'temperature': 0.1, 'max_tokens': 1500, 'reasoning_effort': ('low' if ('@Prompt@' in AT_AFRICAN_TOOL_PROMPT) else 'minimal')},
            'grammar': grammar_prompt_config,
            'legitimacy': legitimacy_prompt_config,
            'repetition': new_repetition_prompt_config,
            'threatening': threatening_prompt_config,
            'backed_out': backed_out_prompt_config,
            'closing_message': closing_message_prompt_config,
            'negative_tool_response': at_african_negative_tool_response_config,
            'flow_order': filipina_flow_order_prompt_config,
            'profile_update': filipina_profile_update_prompt_config,
        },
        'AT_Filipina_Outside_UAE': {
            # 'SA_prompt': sa_prompt_config,
            'loss_interest': loss_interest_outside_config,
            'policy_violation': policy_violation_prompt_config,
            # 'tool': {**tool_prompt_config, 'system_prompt': AT_AFRICAN_TOOL_PROMPT, 'conversion_type': 'json', 'model_type': 'openai', 'model': 'gpt-5-mini', 'temperature': 0.1, 'max_tokens': 1500, 'reasoning_effort': ('low' if ('@Prompt@' in AT_AFRICAN_TOOL_PROMPT) else 'minimal')},
            'grammar': grammar_prompt_config,
            'legitimacy': legitimacy_prompt_config,
            'repetition': new_repetition_prompt_config,
            'threatening': threatening_prompt_config,
            'backed_out': backed_out_prompt_config,
            'closing_message': closing_message_prompt_config,
            'negative_tool_response': at_african_negative_tool_response_config,
            'flow_order': filipina_flow_order_prompt_config,
            'profile_update': filipina_profile_update_prompt_config,
        },
        'AT_Filipina_Inside_UAE': {
            # 'SA_prompt': sa_prompt_config,
            'policy_violation': policy_violation_prompt_config,
            # 'tool': {**tool_prompt_config, 'system_prompt': AT_AFRICAN_TOOL_PROMPT, 'conversion_type': 'json', 'model_type': 'openai', 'model': 'gpt-5-mini', 'temperature': 0.1, 'max_tokens': 1500, 'reasoning_effort': ('low' if ('@Prompt@' in AT_AFRICAN_TOOL_PROMPT) else 'minimal')},
            'grammar': grammar_prompt_config,
            'legitimacy': legitimacy_prompt_config,
            'repetition': new_repetition_prompt_config,
            'threatening': threatening_prompt_config,
            'backed_out': backed_out_prompt_config,
            'closing_message': closing_message_prompt_config,
            'negative_tool_response': at_african_negative_tool_response_config,
            'flow_order': filipina_flow_order_prompt_config,
            'profile_update': filipina_profile_update_prompt_config,
        },
        'AT_African': {
            # 'SA_prompt': sa_prompt_config,
            'loss_interest': {
                **loss_interest_base_config,
                'system_prompt': AT_AFRICAN_LOSS_INTEREST_PROMPT,
                'model': 'gemini-2.5-flash',
                'model_type': 'gemini',
                'skills_cw': {
                    "Kenyan Assessment": 4,
                    "Kenyan Client Scenario": 3,
                    "Kenyan Profile Picture Collection": 20,
                    "Kenyan Attested Pending WP": 20,
                    "Kenyan WP Approved": 40
                }
            },
            'transfer' : transfer_prompt_config,
            'policy_violation': at_african_policy_violation_prompt_config,
            # 'tool': {**tool_prompt_config,
            #     'system_prompt': AT_AFRICAN_TOOL_PROMPT,
            #     'conversion_type': 'json',
            #     'model_type': 'openai',
            #     'model': 'gpt-5-mini',
            #     'temperature': 0.1,
            #     'max_tokens': 1500,
            #     'reasoning_effort': ('low' if ('@Prompt@' in AT_AFRICAN_TOOL_PROMPT) else 'minimal')
            # },
            'grammar': grammar_prompt_config,
            'legitimacy': legitimacy_prompt_config,
            'repetition': new_repetition_prompt_config,
            'threatening': threatening_prompt_config,
            'backed_out': backed_out_prompt_config,
            'closing_message': closing_message_prompt_config,
            'negative_tool_response': at_african_negative_tool_response_config,
            'flow_order': kenyan_flow_order_prompt_config,
            'profile_update': kenyan_profile_update_prompt_config,
            'tool_eval': {
                **tool_eval_prompt_config, 'system_prompt': AT_AFRICAN_TOOL_PROMPT,
                'model': 'gemini-2.5-flash',
                'model_type': 'gemini',
                'temperature': 0,
                'max_tokens': 40000,
                'output_table': 'TOOL_EVAL_RAW_DATA',
                'reasoning_effort': 'minimal',
                'dynamic_prompt_udf': 'BUILD_TOOL_EVAL_SYSTEM_PROMPT_AT_AFRICAN',
                },
           # 'incorrect_assessment_1': incorrect_assessment_1_prompt_config,
           # 'incorrect_assessment_2': incorrect_assessment_2_prompt_config,
           # 'poke_check': poke_check_prompt_config,
           # 'facephoto_analysis': facephoto_analysis_prompt_config,
           # 'passport_analysis': passport_analysis_prompt_config,
           # 'mfa_analysis': mfa_analysis_prompt_config,
           # 'gcc_analysis': gcc_analysis_prompt_config,
        },
        'Gulf_maids': {
            # 'SA_prompt': sa_prompt_config,
            'loss_interest': {
                **loss_interest_base_config,
                'system_prompt': GULF_MAIDS_LOSS_INTEREST_PROMPT,
                'conversion_type': 'json',
                'model_type': 'openai',
                'model': 'gpt-5-mini',
                'temperature': 0,
                'max_tokens': 40000
            },
            'clarification': {
                **loss_interest_base_config,
                'system_prompt': GULF_MAIDS_CLARIFICATION_PROMPT,
                'conversion_type': 'json',
                'model_type': 'openai',
                'model': 'gpt-5-mini',
                'temperature': 0,
                'max_tokens': 40000,
                'output_table': 'CLARIFICATION_RAW_DATA'
            },
            'tool': {
                **loss_interest_base_config,
                'system_prompt': GULF_MAIDS_TOOL_PROMPT,
                'conversion_type': 'json',
                'model_type': 'openai',
                'model': 'gpt-5-mini',
                'temperature': 0,
                'max_tokens': 40000,
                'output_table': 'TOOL_RAW_DATA'
            },
            # Task 37 — Broadcast Messages CVR Checker (Conversion)
            'broadcast_cvr': {
                'prompt': "",
                'system_prompt': BROADCAST_CVR_PROMPT,
                'conversion_type': 'json',  # Simple text output: "Converted: Yes" or "Converted: No"
                'model_type': 'openai',
                'model': 'gpt-5-mini',
                'temperature': 0,
                'max_tokens': 1000,
                'output_table': 'BROADCAST_CVR_RAW_DATA',
                'reasoning_effort': 'low',
                'response_format': 'text'  # Allow raw text response
            },
            # Task 37 — Broadcast Messages Reaction Checker (Positive/Negative)
            'broadcast_reaction': {
                'prompt': "",
                'system_prompt': BROADCAST_REACTION_PROMPT,
                'conversion_type': 'json',  # Simple text output: "Replied: Positively" or "Replied: Negatively"
                'model_type': 'openai',
                'model': 'gpt-5-mini',
                'temperature': 0,
                'max_tokens': 1000,
                'output_table': 'BROADCAST_REACTION_RAW_DATA',
                'reasoning_effort': 'low',
                'response_format': 'text'  # Allow raw text response
            },
        },
        # 'AT_Ethiopian': {
        #     'SA_prompt': sa_prompt_config,
        #     'transfer' : {**transfer_prompt_config, 'system_prompt': AT_ETHIOPIAN_TRANSFER_PROMPT, 'reasoning_effort': ('low' if ('@Prompt@' in AT_ETHIOPIAN_TRANSFER_PROMPT) else 'minimal')},
        #     'loss_interest': {
        #         **loss_interest_base_config, 
        #         'system_prompt': AT_ETHIOPIAN_LOSS_INTEREST_PROMPT,
        #         'model': 'gemini-2.5-flash',
        #         'model_type': 'gemini',
        #         'skills_cw': {
        #             "Ethiopian Assessment": 2,
        #             "Ethiopian Passed Question Assessment": 2,
        #             "Ethiopian Client Scenario": 10
        #         }
        #     },
        #     'policy_violation': {**policy_violation_prompt_config,
        #         'conversion_type': 'xml',  # Fixed: changed from 'xml' to 'json' for proper JSON evaluation
        #         'system_prompt': AT_ETHIOPIAN_POLICY_PORMPT,
        #         'model_type': 'openai',
        #         'model': 'gpt-4o',
        #         'temperature': 0.1,
        #         'max_tokens': 1500,
        #         'reasoning_effort': ('low' if ('@Prompt@' in AT_ETHIOPIAN_POLICY_PORMPT) else 'minimal')
        #     },
        #     'tool': {**tool_prompt_config,
        #         'system_prompt': AT_ETHIOPIAN_TOOL_PROMPT,
        #         'conversion_type': 'xml',
        #         'model_type': 'openai',
        #         'model': 'gpt-4o',
        #         'temperature': 0.1,
        #         'max_tokens': 1500,
        #         'reasoning_effort': ('low' if ('@Prompt@' in AT_ETHIOPIAN_TOOL_PROMPT) else 'minimal')
        #     },
        #     'legitimacy': legitimacy_prompt_config,
        #     'threatening': threatening_prompt_config
        # },
        'CC_Sales': {
            # 'SA_prompt': sa_prompt_config,
            # 'client_suspecting_ai': { **client_suspecting_ai_prompt_config, 'system_prompt': SALES_CLIENT_SUSPECTING_AI_PROMPT, 'model': 'gemini-2.5-flash', 'model_type': 'gemini' },
            'clarity_score': { **clarity_score_prompt_config, 'system_prompt': SALES_CLARITY_SCORE_PROMPT, 'model': 'gemini-2.5-flash', 'model_type': 'gemini' },
            # 'sales_transfer': sales_transfer_prompt_config,
            'tool': {**tool_prompt_config, 'system_prompt': CC_SALES_TOOL_PROMPT, 'reasoning_effort': 'minimal'},
            'policy_violation': cc_sales_policy_violation_prompt_config,
            # 'agent_intervention': cc_sales_agent_intervention_prompt_config,
            'wrong_answer': {**sales_wrong_answer_prompt_config, 'system_prompt': CC_SALES_WRONG_ANSWER_PROMPT},
            'unsatisfactory_policy': {**sales_unsatisfactory_policy_prompt_config, 'system_prompt': CC_SALES_UNSATISFACTORY_POLICY_PROMPT},
            'dissatisfaction': dissatisfaction_prompt_config,
            'de_escalation': de_escalation_prompt_config,
            # 'threatening': threatening_prompt_config
        },
        'MV_Sales': {
            # 'SA_prompt': sa_prompt_config,
            # 'client_suspecting_ai': { **client_suspecting_ai_prompt_config, 'system_prompt': SALES_CLIENT_SUSPECTING_AI_PROMPT, 'model': 'gemini-2.5-flash', 'model_type': 'gemini' },
            'clarity_score': { **clarity_score_prompt_config, 'system_prompt': SALES_CLARITY_SCORE_PROMPT, 'model': 'gemini-2.5-flash', 'model_type': 'gemini', 'response_format': 'json_object' },
            # 'sales_transfer': {**sales_transfer_prompt_config, 'system_prompt': MV_SALES_TRANSFER_PROMPT},
            # 'tool': {**tool_prompt_config, 'system_prompt': MV_SALES_TOOL_PROMPT, 'reasoning_effort': 'minimal'},
            'policy_violation': {**cc_sales_policy_violation_prompt_config, 'system_prompt': MV_SALES_POLICY_VIOLATION_PROMPT},
            'wrong_answer': sales_wrong_answer_prompt_config,
            'unsatisfactory_policy': sales_unsatisfactory_policy_prompt_config,
            'dissatisfaction': dissatisfaction_prompt_config,
            'de_escalation': de_escalation_prompt_config,
            # 'threatening': threatening_prompt_config,
            'tool_eval': tool_eval_prompt_config,
        },
        # Task 39 — Prospect Nationality Service (Sales Nationality Identification Flow)
        'Prospect_Nationality_Service': {
            # Prompt 1: Expected Destination & TransferTool (routing identification)
            'routing_identification': {
                'prompt': "",
                'system_prompt': PROMPT_EXPECTED_DESTINATION_AND_TRANSFERTOOL,
                'conversion_type': 'xml',
                'model_type': 'openai',
                'model': 'gpt-5',
                'temperature': 0.0,
                'max_tokens': 30000,
                'output_table': 'PROSPECT_ROUTING_RAW_DATA',
                'reasoning_effort': ('low' if ('@Prompt@' in PROMPT_EXPECTED_DESTINATION_AND_TRANSFERTOOL) else 'minimal'),
                'response_format': 'json_object'
            },
            # Prompt 2: Engagement Outcomes (No Reply, Drop-off Q1/Q2)
            'engagement_outcomes': {
                'prompt': "",
                'system_prompt': PROMPT_ENGAGEMENT_OUTCOMES,
                'conversion_type': 'xml',
                'model_type': 'openai',
                'model': 'gpt-5',
                'temperature': 0.0,
                'max_tokens': 30000,
                'output_table': 'PROSPECT_ENGAGEMENT_RAW_DATA',
                'reasoning_effort': ('low' if ('@Prompt@' in PROMPT_ENGAGEMENT_OUTCOMES) else 'minimal'),
                'response_format': 'json_object'
            }
        }
    }


def get_snowflake_llm_departments_config():
    """
    Combined configuration: base department config + LLM prompts
    """
    base_config = get_snowflake_base_departments_config()
    llm_prompts = get_llm_prompts_config()
    
    # Merge configurations
    llm_config = {}
    for dept_name, base_dept_config in base_config.items():
        llm_config[dept_name] = {
            **base_dept_config,
            'llm_prompts': llm_prompts.get(dept_name, {})
        }
    
    return llm_config


def get_department_prompt_types(department_name):
    """
    Get all available prompt types for a specific department
    """
    config = get_llm_prompts_config()
    return list(config.get(department_name, {}).keys())


def get_prompt_config(department_name, prompt_type):
    """
    Get specific prompt configuration
    """
    config = get_llm_prompts_config()
    return config.get(department_name, {}).get(prompt_type, None)


def list_all_departments():
    """
    Get list of all configured departments
    """
    return list(get_snowflake_base_departments_config().keys())


def list_all_output_tables():
    """
    Get list of all LLM output tables across all departments
    """
    tables = []
    prompts_config = get_llm_prompts_config()
    
    for dept_name, dept_prompts in prompts_config.items():
        for prompt_type, prompt_config in dept_prompts.items():
            tables.append(prompt_config['output_table'])
    
    return sorted(list(set(tables)))  # Remove duplicates and sort


def _get_metric_func(func_name: str):
    """
    Get a metrics function by name, with fallback to string name.
    
    When running in Snowflake: Returns actual function reference
    When running externally: Returns function name as string
    
    This allows get_metrics_configuration() to work in both environments.
    """
    if _METRICS_CALC_AVAILABLE:
        return _METRICS_FUNCTIONS.get(func_name, func_name)
    return func_name


def get_metrics_configuration():
    """
    Define metrics calculation mapping per department
    Maps departments to their specific metrics calculations and master tables
    
    Note: When running externally (no snowpark), 'function' values will be
    string function names instead of actual function references.
    Use is_metrics_calc_available() to check and get_metrics_function() to
    resolve function names to actual functions when needed.
    """
    
    return {
        'MV_Resolvers': {
            'master_table': 'MV_RESOLVERS_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                'client_suspecting_ai': {
                    'function': _get_metric_func('calculate_client_suspecting_ai_percentage'),
                    'columns': ['CLIENT_SUSPECTING_AI_COUNT', 'CLIENT_SUSPECTING_AI_PERCENTAGE', 'CLIENT_SUSPECTING_AI_DENOMINATOR', 'CLIENT_SUSPECTING_AI_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['client_suspecting_ai'],
                    'order': 2
                },
                'legal_metrics': {
                    'function': _get_metric_func('calculate_legal_metrics'),
                    'columns': ['ESCALATION_RATE', 'ESCALATION_COUNT', 'ESCALATION_DENOMINATOR', 'LEGAL_CONCERNS_PERCENTAGE', 'LEGAL_CONCERNS_COUNT', 'LEGAL_CONCERNS_DENOMINATOR', 'LEGAL_ALIGNMENT_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['legal_alignment'],
                    'order': 3
                },
                'call_request_metrics': {
                    'function': _get_metric_func('calculate_call_request_metrics'),
                    'columns': ['CALL_REQUEST_RATE', 'CALL_REQUEST_COUNT', 'CALL_REQUEST_DENOMINATOR', 'CALL_REQUEST_ANALYSIS_SUMMARY', 'REBUTTAL_RESULT_RATE', 'REBUTTAL_RESULT_DENOMINATOR', 'NO_RETENTION_COUNT'],
                    'depends_on_prompts': ['call_request'],
                    'order': 4
                },
                'intervention_transfer': {
                    'function': _get_metric_func('calculate_overall_percentages'),
                    'columns': ['INTERVENTION_PERCENTAGE', 'INTERVENTION_COUNT', 'INTERVENTION_DENOMINATOR', 'TRANSFER_PERCENTAGE', 'TRANSFER_COUNT', 'TRANSFER_DENOMINATOR', 'INTERVENTION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['categorizing'],
                    'order': 5
                },
                'false_promises': {
                    'function': _get_metric_func('calculate_false_promises_percentage'),
                    'columns': ['FALSE_PROMISES_PERCENTAGE', 'FALSE_PROMISES_COUNT', 'FALSE_PROMISES_DENOMINATOR', 'FALSE_PROMISES_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['false_promises'],
                    'order': 6
                },
                'ftr': {
                    'function': _get_metric_func('calculate_ftr_percentage'),
                    'columns': ['FTR_PERCENTAGE', 'FTR_COUNT', 'FTR_DENOMINATOR', 'FTR_REACHED_OUT_2_TIMES', 'FTR_REACHED_OUT_3_TIMES', 'FTR_REACHED_OUT_MORE_THAN_3_TIMES', 'FTR_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['ftr'],
                    'order': 7
                },
                'threatening': {
                    'function': _get_metric_func('calculate_threatening_percentage'),
                    'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['threatening'],
                    'order': 8
                },
                'policy_escalation': {
                    'function': _get_metric_func('calculate_policy_escalation_percentage'),
                    'columns': ['POLICY_ESCALATION_PERCENTAGE', 'POLICY_ESCALATION_COUNT', 'POLICY_ESCALATION_DENOMINATOR', 'POLICY_ESCALATION_ANALYSIS_SUMMARY', 'TRANSFER_DUE_TO_ESCALATION_PERCENTAGE', 'TRANSFER_DUE_TO_ESCALATION_COUNT', 'TRANSFER_DUE_TO_ESCALATION_DENOMINATOR'],
                    'depends_on_prompts': ['policy_escalation'],
                    'order': 9
                },
                # 'clarity_score': {
                #     'function': _get_metric_func('calculate_clarity_score_percentage'),
                #     'columns': ['CLARITY_SCORE_PERCENTAGE', 'CLARITY_SCORE_COUNT', 'CLARITY_SCORE_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['clarity_score'],
                #     'order': 10
                # },
                # 'system_prompt_token_count': {
                #     'function': create_system_prompt_token_summary_report,
                #     'columns': ['SYSTEM_PROMPT_TOKEN_COUNT'],
                #     'depends_on_prompts': ['policy_escalation'],
                #     'order': 11
                # },
                'mv_resolvers_wrong_tool': {
                    'function': _get_metric_func('generate_mv_resolvers_wrong_tool_summary_report'),
                    'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'WRONG_TOOL_DENOMINATOR', 'WRONG_TOOL_ANALYSIS_SUMMARY', 'WRONG_TOOL_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['mv_resolvers_wrong_tool'],
                    'order': 12
                },
                'mv_resolvers_missing_tool': {
                    'function': _get_metric_func('generate_mv_resolvers_missing_tool_summary_report'),
                    'columns': ['MISSING_TOOL_PERCENTAGE', 'MISSING_TOOL_COUNT', 'MISSING_TOOL_DENOMINATOR', 'MISSING_TOOL_ANALYSIS_SUMMARY', 'MISSING_TOOL_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['mv_resolvers_missing_tool'],
                    'order': 13
                },
                # 'shadowing_automation_summary': {
                #     'function': create_shadowing_automation_summary_report,
                #     'columns': ['SHADOWING_AUTOMATION_ANALYSIS_SUMMARY','SHADOWING_AUTOMATION_SUMMARY_SUCCESS'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 14
                # },
                'missing_policy': {
                    'function': _get_metric_func('calculate_missing_policy_metrics'),
                    'columns': ['MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT', 'MISSING_POLICY_DENOMINATOR', 'MISSING_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['missing_policy'],
                    'order': 15
                },
                'unclear_policy': {
                    'function': _get_metric_func('calculate_unclear_policy_metrics'),
                    'columns': ['UNCLEAR_POLICY_PERCENTAGE', 'UNCLEAR_POLICY_COUNT', 'UNCLEAR_POLICY_DENOMINATOR', 'UNCLEAR_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['unclear_policy'],
                    'order': 16
                },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 17
                },
                # 'client_mention_another_maid': {
                #     'function': _get_metric_func('calculate_client_mention_another_maid_percentage'),
                #     'columns': ['CLIENT_MENTION_ANOTHER_MAID_PERCENTAGE', 'CLIENT_MENTION_ANOTHER_MAID_COUNT', 'CLIENT_MENTION_ANOTHER_MAID_DENOMINATOR', 'CLIENT_MENTION_ANOTHER_MAID_ANALYSIS_SUMMARY', 'ANOTHER_MAID_TOOL_SUCCESS_PERCENTAGE', 'ANOTHER_MAID_TOOL_SUCCESS_COUNT', 'ANOTHER_MAID_TOOL_SUCCESS_DENOMINATOR'],
                #     'depends_on_prompts': ['client_mention_another_maid'],
                #     'order': 18
                # }
                
            }
        },
        'Routing_Bot': {
            'master_table': 'ROUTING_BOT_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                'false_promises': {
                    'function': _get_metric_func('calculate_routing_bot_false_promises_percentage'),
                    'columns': ['FALSE_PROMISES_PERCENTAGE', 'FALSE_PROMISES_COUNT', 'FALSE_PROMISES_DENOMINATOR', 'FALSE_PROMISES_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['false_promises'],
                    'order': 2
                },
                'call_request_metrics': {
                    'function': _get_metric_func('calculate_call_request_metrics'),
                    'columns': ['CALL_REQUEST_RATE', 'CALL_REQUEST_COUNT', 'CALL_REQUEST_DENOMINATOR', 'CALL_REQUEST_ANALYSIS_SUMMARY', 'REBUTTAL_RESULT_RATE', 'REBUTTAL_RESULT_DENOMINATOR', 'NO_RETENTION_COUNT'],
                    'depends_on_prompts': ['call_request'],
                    'order': 3
                },
                'tool': {
                    'function': _get_metric_func('generate_routing_bot_tool_summary_report'),
                    'columns': ['WRONG_TOOL_PERCENTAGE', 'TOOL_CALL_CORRECT_COUNT', 'TOOL_CALL_MADE_COUNT', 'TOOL_ANALYSIS_SUMMARY', 'TOOL_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['tool'],
                    'order': 4
                },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 5
                },
                'threatening': {
                    'function': _get_metric_func('calculate_threatening_percentage'),
                    'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['threatening'],
                    'order': 6
                },
            }
        },
        'CC_Resolvers': {
            'master_table': 'CC_RESOLVERS_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 2
                },
                'client_suspecting_ai': {
                    'function': _get_metric_func('calculate_client_suspecting_ai_percentage'),
                    'columns': ['CLIENT_SUSPECTING_AI_COUNT', 'CLIENT_SUSPECTING_AI_PERCENTAGE', 'CLIENT_SUSPECTING_AI_DENOMINATOR', 'CLIENT_SUSPECTING_AI_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['client_suspecting_ai'],
                    'order': 3
                },
                'call_request_metrics': {
                    'function': _get_metric_func('calculate_call_request_metrics_cc_resolvers'),
                    'columns': ['CALL_REQUEST_RATE', 'CALL_REQUEST_COUNT', 'CALL_REQUEST_ANALYSIS_SUMMARY', 'REBUTTAL_RESULT_RATE', 'NO_RETENTION_COUNT', 'MULTIPLE_CALL_REQUEST_RATE'],
                    'depends_on_prompts': ['call_request'],
                    'order': 4
                },
                'questioning_legalities': {
                    'function': _get_metric_func('calculate_questioning_legalities_metrics'),
                    'columns': [
                        'CLIENTS_QUESTIONING_LEGALITIES_ESCALATED_PERCENTAGE',
                        'CLIENTS_QUESTIONING_LEGALITIES_ESCALATED_COUNT',
                        'CLIENTS_QUESTIONING_LEGALITIES_ESCALATED_DENOMINATOR',
                        'CLIENTS_QUESTIONING_LEGALITIES_PERCENTAGE',
                        'CLIENTS_QUESTIONING_LEGALITIES_COUNT',
                        'CLIENTS_QUESTIONING_LEGALITIES_DENOMINATOR',
                        'CLIENTS_QUESTIONING_LEGALITIES_ANALYSIS_SUMMARY',
                    ],
                    'depends_on_prompts': ['questioning_legalities'],
                    'order': 40
                },
                'false_promises': {
                    'function': _get_metric_func('calculate_false_promises_percentage'),
                    'columns': ['FALSE_PROMISES_PERCENTAGE', 'FALSE_PROMISES_COUNT', 'FALSE_PROMISES_DENOMINATOR', 'FALSE_PROMISES_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['false_promises'],
                    'order': 41
                },
                'wrong_answer': {
                    'function': _get_metric_func('calculate_wrong_answer_metrics'),
                    'columns': ['PROMPT_BASED_WRONG_ANSWER_PERCENTAGE', 'PROMPT_BASED_WRONG_ANSWER_COUNT', 'OVERALL_WRONG_ANSWER_PERCENTAGE', 'OVERALL_WRONG_ANSWER_COUNT', 'WRONG_ANSWER_DENOMINATOR', 'WRONG_ANSWER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['wrong_answer'],
                    'order': 42
                },
                'exceptions_granted': {
                    'function': _get_metric_func('calculate_exceptions_granted_percentage'),
                    'columns': ['EXCEPTION_COUNT', 'EXCEPTION_PERCENTAGE', 'EXCEPTION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['exceptions_granted'],
                    'order': 5
                },
                'policy_transfer': {
                    'function': _get_metric_func('generate_policy_transfer_summary_report'),
                    'columns': ['POLICY_TRANSFER_SUMMARY_SUCCESS', 'POLICY_TRANSFER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['policy_transfer'],
                    'order': 6
                },
                'threatening': {
                    'function': _get_metric_func('calculate_threatening_percentage'),
                    'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['threatening'],
                    'order': 7
                },
                # 'threatening_case_identifier': {
                #     'function': _get_metric_func('calculate_threatening_case_identifier_percentage'),
                #     'columns': ['THREATENING_CASE_IDENTIFIER_PERCENTAGE', 'THREATENING_CASE_IDENTIFIER_COUNT', 'THREATENING_CASE_IDENTIFIER_DENOMINATOR', 'THREATENING_CASE_IDENTIFIER_SUMMARY_SUCCESS', 'THREATENING_CASE_IDENTIFIER_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['threatening_case_identifier'],
                #     'order': 8
                # },
                'promise_no_tool_triggered': {
                    'function': _get_metric_func('calculate_promise_no_tool_triggered_percentage'),
                    'columns': ['PROMISE_NO_TOOL_TRIGGERED_PERCENTAGE_A', 'PROMISE_NO_TOOL_TRIGGERED_PERCENTAGE_B', 'PROMISE_NO_TOOL_TRIGGERED_COUNT', 'PROMISE_NO_TOOL_TRIGGERED_DENOMINATOR_A', 'PROMISE_NO_TOOL_TRIGGERED_DENOMINATOR_B', 'PROMISE_NO_TOOL_TRIGGERED_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['promise_no_tool_triggered'],
                    'order': 9
                },
                'clarification': {
                    'function': _get_metric_func('calculate_clarification_percentage'),
                    'columns': ['CLARIFICATION_PERCENTAGE', 'CLARIFICATION_COUNT', 'CLARIFICATION_DENOMINATOR', 'CLARIFICATION_SUMMARY_SUCCESS', 'CLARIFICATION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['clarification'],
                    'order': 10
                },
                'request_retraction': {
                    'function': _get_metric_func('generate_request_retraction_summary_report'),
                    'columns': ['REQUEST_RETRACTION_SUMMARY_SUCCESS', 'REQUEST_RETRACTION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['request_retraction'],
                    'order': 11
                },
                'replacement_retraction_breakdown': {
                    'function': _get_metric_func('generate_replacement_request_retraction_breakdown_report'),
                    'columns': ['REPLACEMENT_REQUEST_RETRACTION_BREAKDOWN_SUCCESS', 'REPLACEMENT_REQUEST_RETRACTION_BREAKDOWN_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['request_retraction'],
                    'order': 11.5
                },
                # Task 38 — CC Resolvers — Cancellation Request Reasons & Retraction (breakdown table)
                'cancellation_retraction_breakdown': {
                    'function': _get_metric_func('generate_cancellation_request_retraction_breakdown_report'),
                    'columns': ['CANCELLATION_REQUEST_RETRACTION_BREAKDOWN_SUCCESS', 'CANCELLATION_REQUEST_RETRACTION_BREAKDOWN_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['cancellation_retraction'],
                    'order': 11.6
                },
                'document_request_failure': {
                    'function': _get_metric_func('calculate_document_request_failure_chat_percentage'),
                    'columns': ['DOCUMENT_REQUEST_FAILURE_PERCENTAGE', 'DOCUMENT_REQUEST_FAILURE_COUNT', 'DOCUMENT_REQUEST_FAILURE_DENOMINATOR', 'DOCUMENT_REQUEST_FAILURE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['document_request_failure'],
                    'order': 11.7
                },
                'wrong_tool': {
                    'function': _get_metric_func('generate_cc_delighters_wrong_tool_summary_report'),
                    'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'WRONG_TOOL_DENOMINATOR', 'WRONG_TOOL_ANALYSIS_SUMMARY', 'WRONG_TOOL_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['wrong_tool'],
                    'order': 12
                },
                'missing_tool': {
                    'function': _get_metric_func('generate_mv_resolvers_missing_tool_summary_report'),
                    'columns': ['MISSING_TOOL_PERCENTAGE', 'MISSING_TOOL_COUNT', 'MISSING_TOOL_DENOMINATOR', 'MISSING_TOOL_ANALYSIS_SUMMARY', 'MISSING_TOOL_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['missing_tool'],
                    'order': 13
                },
                'missing_policy': {
                    'function': _get_metric_func('calculate_missing_policy_metrics'),
                    'columns': ['MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT', 'MISSING_POLICY_DENOMINATOR', 'MISSING_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['missing_policy'],
                    'order': 14
                },
                'unclear_policy': {
                    'function': _get_metric_func('calculate_cc_resolvers_unclear_policy_percentage'),
                    'columns': ['UNCLEAR_POLICY_PERCENTAGE', 'UNCLEAR_POLICY_COUNT', 'UNCLEAR_POLICY_DENOMINATOR', 'UNCLEAR_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['unclear_policy'],
                    'order': 15
                },
                'categorizing_metrics': {
                    'function': _get_metric_func('calculate_cc_resolvers_categorizing_metrics'),
                    'columns': ['TRANSFERS_DUE_TO_KNOWN_FLOWS_PERCENTAGE', 'TRANSFERS_DUE_TO_KNOWN_FLOWS_COUNT', 'TRANSFERS_DUE_TO_KNOWN_FLOWS_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['categorizing'],
                    'order': 16
                },
                'transfers_due_to_escalations': {
                    'function': _get_metric_func('calculate_cc_resolvers_transfers_due_to_escalations'),
                    'columns': [
                        'TRANSFERS_DUE_TO_ESCALATIONS_PERCENTAGE',
                        'TRANSFERS_DUE_TO_ESCALATIONS_OF_TRANSFERS_PERCENTAGE',
                        'TRANSFERS_DUE_TO_ESCALATIONS_COUNT',
                        'TRANSFERS_DUE_TO_ESCALATIONS_DENOMINATOR',
                        'TRANSFERS_DUE_TO_ESCALATIONS_TRANSFER_DENOMINATOR',
                        'TRANSFERS_DUE_TO_ESCALATIONS_ANALYSIS_SUMMARY',
                    ],
                    'depends_on_prompts': ['policy_escalation'],
                    'order': 16
                },
                'unsatisfactory_policy': {
                    'function': _get_metric_func('calculate_cc_resolvers_unsatisfactory_policy_percentage'),
                    'columns': ['UNSATISFACTORY_POLICY_PERCENTAGE', 'UNSATISFACTORY_POLICY_COUNT', 'UNSATISFACTORY_POLICY_DENOMINATOR', 'UNSATISFACTORY_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['policy_escalation'],
                    'order': 17
                },
                # 'shadowing_automation_summary': {
                #     'function': create_shadowing_automation_summary_report,
                #     'columns': ['SHADOWING_AUTOMATION_ANALYSIS_SUMMARY','SHADOWING_AUTOMATION_SUMMARY_SUCCESS'],
                #     'depends_on_prompts': [],
                #     'order': 15
                # },
                'ftr': {
                    'function': _get_metric_func('calculate_ftr_percentage'),
                    'columns': ['FTR_PERCENTAGE', 'FTR_COUNT', 'FTR_DENOMINATOR', 'FTR_REACHED_OUT_2_TIMES', 'FTR_REACHED_OUT_3_TIMES', 'FTR_REACHED_OUT_MORE_THAN_3_TIMES', 'FTR_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['ftr'],
                    'order': 7
                },
            }
        },
        'Doctors': {
            'master_table': 'DOCTORS_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                'misprescription': {
                    'function': _get_metric_func('calculate_misprescription_percentage'),
                    'columns': ['MISPRESCRIPTION_PERCENTAGE', 'MISPRESCRIPTION_COUNT', 'MISPRESCRIPTION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['misprescription'],
                    'order': 2
                },
                'unnecessary_clinic': {
                    'function': _get_metric_func('calculate_unnecessary_clinic_percentage'),
                    'columns': ['UNNECESSARY_CLINIC_PERCENTAGE', 'COULD_AVOID_COUNT', 'UNNECESSARY_CLINIC_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['unnecessary_clinic'],
                    'order': 3
                },
                # 'clarity_score': {
                #     'function': _get_metric_func('calculate_clarity_score_percentage'),
                #     'columns': ['CLARITY_SCORE_PERCENTAGE', 'CLARITY_SCORE_COUNT', 'CLARITY_SCORE_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['clarity_score'],
                #     'order': 4
                # },
                'categorizing_summary': {
                    'function': create_doctors_categorizing_summary_report,
                    'columns': ['CATEGORIZING_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['doctors_categorizing'],
                    'order': 5
                },
                # 'policy_escalation': {
                #     'function': _get_metric_func('calculate_policy_escalation_percentage'),
                #     'columns': ['POLICY_ESCALATION_PERCENTAGE', 'POLICY_ESCALATION_COUNT', 'POLICY_ESCALATION_DENOMINATOR', 'POLICY_ESCALATION_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['policy_escalation'],
                #     'order': 6
                # },
                'client_suspecting_ai': {
                    'function': _get_metric_func('calculate_client_suspecting_ai_percentage'),
                    'columns': ['CLIENT_SUSPECTING_AI_COUNT', 'CLIENT_SUSPECTING_AI_PERCENTAGE', 'CLIENT_SUSPECTING_AI_DENOMINATOR', 'CLIENT_SUSPECTING_AI_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['client_suspecting_ai'],
                    'order': 7
                },
                # 'system_prompt_token_count': {
                #     'function': create_system_prompt_token_summary_report,
                #     'columns': ['SYSTEM_PROMPT_TOKEN_COUNT'],
                #     'depends_on_prompts': ['policy_escalation'],
                #     'order': 8
                # },
                'intervention_transfer': {
                    'function': _get_metric_func('calculate_overall_percentages'),
                    'columns': ['INTERVENTION_PERCENTAGE', 'INTERVENTION_COUNT', 'INTERVENTION_DENOMINATOR', 'TRANSFER_PERCENTAGE', 'TRANSFER_COUNT', 'TRANSFER_DENOMINATOR', 'INTERVENTION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['intervention'],
                    'order': 9
                },
                # 'clinic_recommendation_reason': {
                #     'function': create_clinic_reasons_summary_report,
                #     'columns': ['CLINIC_RECOMMENDATION_REASON_SUMMARY_SUCCESS'],
                #     'depends_on_prompts': ['clinic_recommendation_reason'],
                #     'order': 10
                # },
                'tool': {
                    'function': _get_metric_func('generate_tool_summary_report'),
                    'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'MISSING_TOOL_PERCENTAGE', 'MISSING_TOOL_COUNT', 'TOOL_ANALYSIS_SUMMARY', 'TOOL_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['tool'],
                    'order': 11
                },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 12
                },
                'transfer_escalation': {
                    'function': _get_metric_func('calculate_doctors_transfer_escalation_percentage'),
                    'columns': ['TRANSFER_ESCALATION_PERCENTAGE', 'TRANSFER_ESCALATION_COUNT', 'TRANSFER_ESCALATION_DENOMINATOR', 'TRANSFER_ESCALATION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['transfer_escalation'],
                    'order': 13
                },
                'transfer_known_flow': {
                    'function': _get_metric_func('calculate_doctors_transfer_known_flow_percentage'),
                    'columns': ['TRANSFER_KNOWN_FLOW_PERCENTAGE', 'TRANSFER_KNOWN_FLOW_COUNT', 'TRANSFER_KNOWN_FLOW_DENOMINATOR', 'TRANSFER_KNOWN_FLOW_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['transfer_known_flow'],
                    'order': 14
                },
                'insurance_complaints': {
                    'function': _get_metric_func('calculate_doctors_insurance_complaints_percentage'),
                    'columns': ['INSURANCE_COMPLAINTS_PERCENTAGE', 'INSURANCE_COMPLAINTS_COUNT', 'INSURANCE_COMPLAINTS_DENOMINATOR', 'INSURANCE_COMPLAINTS_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['insurance_complaints'],
                    'order': 15
                },
                'missing_policy': {
                    'function': _get_metric_func('calculate_doctors_missing_policy_metrics'),
                    'columns': ['MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT', 'MISSING_POLICY_DENOMINATOR', 'MISSING_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['missing_policy'],
                    'order': 16
                },
                'threatening': {
                    'function': _get_metric_func('calculate_threatening_case_identifier_percentage'),
                    'columns': ['THREATENING_CASE_IDENTIFIER_PERCENTAGE', 'THREATENING_CASE_IDENTIFIER_COUNT', 'THREATENING_CASE_IDENTIFIER_DENOMINATOR', 'THREATENING_CASE_IDENTIFIER_SUMMARY_SUCCESS', 'THREATENING_CASE_IDENTIFIER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['threatening'],
                    'order': 15
                },
                'doctor_agent_intervention': {
                    'function': _get_metric_func('calculate_doctor_agent_intervention_percentage'),
                    'columns': ['DOCTOR_AGENT_INTERVENTION_PERCENTAGE', 'DOCTOR_AGENT_INTERVENTION_COUNT', 'DOCTOR_AGENT_INTERVENTION_DENOMINATOR', 'DOCTOR_AGENT_INTERVENTION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['doctor_agent_intervention'],
                    'order': 17
                },
                'document_request': {
                    'function': _get_metric_func('calculate_document_request_failure_percentage'),
                    'columns': ['DOCUMENT_REQUEST_FAILURE_PERCENTAGE', 'DOCUMENT_REQUEST_FAILURE_COUNT', 'DOCUMENT_REQUEST_TOTAL_REQUESTS', 'DOCUMENT_REQUEST_SUCCESS_COUNT', 'DOCUMENT_REQUEST_WRONG_DOC_COUNT', 'DOCUMENT_REQUEST_NOT_SENT_COUNT', 'DOCUMENT_REQUEST_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['document_request'],
                    'order': 18
                },
                'wrong_tool': {
                    'function': _get_metric_func('calculate_doctors_wrong_tool_percentage'),
                    'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'TOTAL_TOOLS_EVALUATED', 'WRONG_TOOL_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['wrong_tool'],
                    'order': 19
                },
                'missing_tool': {
                    'function': _get_metric_func('calculate_doctors_missing_tool_percentage'),
                    'columns': ['MISSING_TOOL_PERCENTAGE', 'MISSING_TOOL_COUNT', 'TOTAL_TOOLS_EVALUATED', 'MISSING_TOOL_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['missing_tool'],
                    'order': 20
                },
                'health_check': {
                    'function': _get_metric_func('calculate_doctors_health_check_response_percentage'),
                    'columns': ['SICK_PERCENTAGE', 'SICK_COUNT', 'OKAY_PERCENTAGE', 'OKAY_COUNT', 'NO_RESPONSE_PERCENTAGE', 'NO_RESPONSE_COUNT', 'TOTAL_HEALTH_CHECKS', 'HEALTH_CHECK_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['health_check'],
                    'order': 21
                },
                'wrong_answer': {
                    'function': _get_metric_func('calculate_wrong_answer_metrics'),
                    'columns': ['PROMPT_BASED_WRONG_ANSWER_PERCENTAGE', 'PROMPT_BASED_WRONG_ANSWER_COUNT', 'OVERALL_WRONG_ANSWER_PERCENTAGE', 'OVERALL_WRONG_ANSWER_COUNT', 'WRONG_ANSWER_DENOMINATOR', 'WRONG_ANSWER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['wrong_answer'],
                    'order': 22
                },
                'ftr': {
                    'function': _get_metric_func('calculate_ftr_percentage'),
                    'columns': ['FTR_PERCENTAGE', 'FTR_COUNT', 'FTR_DENOMINATOR', 'FTR_REACHED_OUT_2_TIMES', 'FTR_REACHED_OUT_3_TIMES', 'FTR_REACHED_OUT_MORE_THAN_3_TIMES', 'FTR_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['ftr'],
                    'order': 23
                },
            }
        },
        'MV_Delighters': {
            'master_table': 'DELIGHTERS_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 2
                },
                'threatening': {
                    'function': _get_metric_func('calculate_threatening_percentage'),
                    'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['threatening'],
                    'order': 3
                },
                'client_suspecting_ai': {
                    'function': _get_metric_func('calculate_client_suspecting_ai_percentage'),
                    'columns': ['CLIENT_SUSPECTING_AI_COUNT', 'CLIENT_SUSPECTING_AI_PERCENTAGE', 'CLIENT_SUSPECTING_AI_DENOMINATOR', 'CLIENT_SUSPECTING_AI_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['client_suspecting_ai'],
                    'order': 4
                },
                'legal_metrics': {
                    'function': _get_metric_func('calculate_legal_metrics'),
                    'columns': ['ESCALATION_RATE', 'ESCALATION_COUNT', 'ESCALATION_DENOMINATOR', 'LEGAL_CONCERNS_PERCENTAGE', 'LEGAL_CONCERNS_COUNT', 'LEGAL_CONCERNS_DENOMINATOR', 'LEGAL_ALIGNMENT_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['legal_alignment'],
                    'order': 5
                },
                'call_request_metrics': {
                    'function': _get_metric_func('calculate_call_request_metrics'),
                    'columns': ['CALL_REQUEST_RATE', 'CALL_REQUEST_COUNT', 'CALL_REQUEST_DENOMINATOR', 'CALL_REQUEST_ANALYSIS_SUMMARY', 'REBUTTAL_RESULT_RATE', 'REBUTTAL_RESULT_DENOMINATOR', 'NO_RETENTION_COUNT'],
                    'depends_on_prompts': ['call_request'],
                    'order': 6
                },
                'intervention_transfer': {
                    'function': _get_metric_func('calculate_overall_percentages'),
                    'columns': ['INTERVENTION_PERCENTAGE', 'INTERVENTION_COUNT', 'INTERVENTION_DENOMINATOR', 'TRANSFER_PERCENTAGE', 'TRANSFER_COUNT', 'TRANSFER_DENOMINATOR', 'INTERVENTION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['categorizing'],
                    'order': 7
                },
                'false_promises': {
                    'function': _get_metric_func('calculate_false_promises_percentage'),
                    'columns': ['FALSE_PROMISES_PERCENTAGE', 'FALSE_PROMISES_COUNT', 'FALSE_PROMISES_DENOMINATOR', 'FALSE_PROMISES_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['false_promises'],
                    'order': 8
                },
                'ftr': {
                    'function': _get_metric_func('calculate_ftr_percentage'),
                    'columns': ['FTR_PERCENTAGE', 'FTR_COUNT', 'FTR_DENOMINATOR', 'FTR_REACHED_OUT_2_TIMES', 'FTR_REACHED_OUT_3_TIMES', 'FTR_REACHED_OUT_MORE_THAN_3_TIMES', 'FTR_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['ftr'],
                    'order': 9
                },
                'policy_escalation': {
                    'function': _get_metric_func('calculate_policy_escalation_percentage'),
                    'columns': ['POLICY_ESCALATION_PERCENTAGE', 'POLICY_ESCALATION_COUNT', 'POLICY_ESCALATION_DENOMINATOR', 'POLICY_ESCALATION_ANALYSIS_SUMMARY', 'TRANSFER_DUE_TO_ESCALATION_PERCENTAGE', 'TRANSFER_DUE_TO_ESCALATION_COUNT', 'TRANSFER_DUE_TO_ESCALATION_DENOMINATOR'],
                    'depends_on_prompts': ['policy_escalation'],
                    'order': 10
                },
                # 'clarity_score': {
                #     'function': _get_metric_func('calculate_clarity_score_percentage'),
                #     'columns': ['CLARITY_SCORE_PERCENTAGE', 'CLARITY_SCORE_COUNT', 'CLARITY_SCORE_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['clarity_score'],
                #     'order': 11
                # },
                # 'system_prompt_token_count': {
                #     'function': create_system_prompt_token_summary_report,
                #     'columns': ['SYSTEM_PROMPT_TOKEN_COUNT'],
                #     'depends_on_prompts': ['policy_escalation'],
                #     'order': 12
                # },
                'mv_resolvers_wrong_tool': {
                    'function': _get_metric_func('generate_mv_resolvers_wrong_tool_summary_report'),
                    'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'WRONG_TOOL_DENOMINATOR', 'WRONG_TOOL_ANALYSIS_SUMMARY', 'WRONG_TOOL_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['tool_eval'],
                    'order': 13
                },
                'mv_resolvers_missing_tool': {
                    'function': _get_metric_func('generate_mv_resolvers_missing_tool_summary_report'),
                    'columns': ['MISSING_TOOL_PERCENTAGE', 'MISSING_TOOL_COUNT', 'MISSING_TOOL_DENOMINATOR', 'MISSING_TOOL_ANALYSIS_SUMMARY', 'MISSING_TOOL_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['mv_resolvers_missing_tool'],
                    'order': 14
                },
                # 'shadowing_automation_summary': {
                #     'function': create_shadowing_automation_summary_report,
                #     'columns': ['SHADOWING_AUTOMATION_ANALYSIS_SUMMARY','SHADOWING_AUTOMATION_SUMMARY_SUCCESS'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 15
                # },
                'missing_policy': {
                    'function': _get_metric_func('calculate_missing_policy_metrics'),
                    'columns': ['MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT', 'MISSING_POLICY_DENOMINATOR', 'MISSING_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['missing_policy'],
                    'order': 16
                },
                'unclear_policy': {
                    'function': _get_metric_func('calculate_unclear_policy_metrics'),
                    'columns': ['UNCLEAR_POLICY_PERCENTAGE', 'UNCLEAR_POLICY_COUNT', 'UNCLEAR_POLICY_DENOMINATOR', 'UNCLEAR_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['unclear_policy'],
                    'order': 17
                }
            }
        },
        'CC_Delighters': {
            'master_table': 'CC_DELIGHTERS_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 2
                },
                'missed_tool': {
                    'function': _get_metric_func('calculate_cc_delighters_missed_tool_percentage'),
                    'columns': ['MISSED_TOOL_PERCENTAGE', 'MISSED_TOOL_COUNT', 'MISSED_TOOL_DENOMINATOR', 'MISSED_TOOL_ANALYSIS_SUMMARY', 'MISSED_TOOL_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['missed_tool'],
                    'order': 3
                },
                'missing_policy': {
                    'function': _get_metric_func('calculate_delighters_missing_policy_metrics'),
                    'columns': ['MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT', 'MISSING_POLICY_DENOMINATOR', 'MISSING_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['missing_policy'],
                    'order': 4
                },
                'wrong_tool': {
                    'function': _get_metric_func('generate_cc_delighters_wrong_tool_summary_report'),
                    'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'WRONG_TOOL_DENOMINATOR', 'WRONG_TOOL_ANALYSIS_SUMMARY', 'WRONG_TOOL_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['wrong_tool'],
                    'order': 5
                },
                'unclear_policy': {
                    'function': _get_metric_func('calculate_unclear_policy_metrics'),
                    'columns': ['UNCLEAR_POLICY_PERCENTAGE', 'UNCLEAR_POLICY_COUNT', 'UNCLEAR_POLICY_DENOMINATOR', 'UNCLEAR_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['unclear_policy'],
                    'order': 6
                },
                'categorizing_metrics': {
                    'function': _get_metric_func('calculate_cc_delighters_categorizing_metrics'),
                    'columns': ['TRANSFERS_DUE_TO_KNOWN_FLOWS_PERCENTAGE', 'TRANSFERS_DUE_TO_KNOWN_FLOWS_COUNT', 'TRANSFERS_DUE_TO_KNOWN_FLOWS_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['categorizing'],
                    'order': 7
                },
                'transfers_due_to_escalations': {
                    'function': _get_metric_func('calculate_cc_delighters_transfers_due_to_escalations'),
                    'columns': [
                        'TRANSFERS_DUE_TO_ESCALATIONS_PERCENTAGE',
                        'TRANSFERS_DUE_TO_ESCALATIONS_OF_TRANSFERS_PERCENTAGE',
                        'TRANSFERS_DUE_TO_ESCALATIONS_COUNT',
                        'TRANSFERS_DUE_TO_ESCALATIONS_DENOMINATOR',
                        'TRANSFERS_DUE_TO_ESCALATIONS_TRANSFER_DENOMINATOR',
                        'TRANSFERS_DUE_TO_ESCALATIONS_ANALYSIS_SUMMARY',
                    ],
                    'depends_on_prompts': ['policy_escalation'],
                    'order': 8
                },
                'unsatisfactory_policy': {
                    'function': _get_metric_func('calculate_cc_delighters_unsatisfactory_policy_percentage'),
                    'columns': ['UNSATISFACTORY_POLICY_PERCENTAGE', 'UNSATISFACTORY_POLICY_COUNT', 'UNSATISFACTORY_POLICY_DENOMINATOR', 'UNSATISFACTORY_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['policy_escalation'],
                    'order': 9
                },
                # Task 17 — CC Delighters — Unique Issue Extractor
                'shadowing_automation_summary': {
                    'function': create_shadowing_automation_summary_report,
                    'columns': ['SHADOWING_AUTOMATION_ANALYSIS_SUMMARY', 'SHADOWING_AUTOMATION_SUMMARY_SUCCESS'],
                    'depends_on_prompts': [],
                    'order': 10
                },
                'call_request_metrics': {
                    'function': _get_metric_func('calculate_call_request_metrics_cc_resolvers'),
                    'columns': ['CALL_REQUEST_RATE', 'CALL_REQUEST_COUNT', 'CALL_REQUEST_ANALYSIS_SUMMARY', 'REBUTTAL_RESULT_RATE', 'NO_RETENTION_COUNT', 'MULTIPLE_CALL_REQUEST_RATE'],
                    'depends_on_prompts': ['call_request'],
                    'order': 11
                },
                'questioning_legalities': {
                    'function': _get_metric_func('calculate_questioning_legalities_metrics'),
                    'columns': [
                        'MAIDS_QUESTIONING_LEGALITIES_ESCALATED_PERCENTAGE',
                        'MAIDS_QUESTIONING_LEGALITIES_ESCALATED_COUNT',
                        'MAIDS_QUESTIONING_LEGALITIES_ESCALATED_DENOMINATOR',
                        'MAIDS_QUESTIONING_LEGALITIES_PERCENTAGE',
                        'MAIDS_QUESTIONING_LEGALITIES_COUNT',
                        'MAIDS_QUESTIONING_LEGALITIES_DENOMINATOR',
                        'MAIDS_QUESTIONING_LEGALITIES_ANALYSIS_SUMMARY',
                    ],
                    'depends_on_prompts': ['questioning_legalities'],
                    'order': 12
                },
                'false_promises': {
                    'function': _get_metric_func('calculate_false_promises_percentage'),
                    'columns': ['FALSE_PROMISES_PERCENTAGE', 'FALSE_PROMISES_COUNT', 'FALSE_PROMISES_DENOMINATOR', 'FALSE_PROMISES_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['false_promises'],
                    'order': 13
                },
                'wrong_answer': {
                    'function': _get_metric_func('calculate_wrong_answer_metrics'),
                    'columns': ['PROMPT_BASED_WRONG_ANSWER_PERCENTAGE', 'PROMPT_BASED_WRONG_ANSWER_COUNT', 'OVERALL_WRONG_ANSWER_PERCENTAGE', 'OVERALL_WRONG_ANSWER_COUNT', 'WRONG_ANSWER_DENOMINATOR', 'WRONG_ANSWER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['wrong_answer'],
                    'order': 13.5
                },
                'complexity_violation': {
                    'function': _get_metric_func('calculate_complexity_violation_percentage'),
                    'columns': ['COMPLEXITY_VIOLATION_PERCENTAGE', 'COMPLEXITY_VIOLATION_COUNT', 'COMPLEXITY_VIOLATION_DENOMINATOR', 'COMPLEXITY_VIOLATION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['complexity_violation'],
                    'order': 14
                },
                'document_request_failure': {
                    'function': _get_metric_func('calculate_document_request_failure_chat_percentage'),
                    'columns': ['DOCUMENT_REQUEST_FAILURE_PERCENTAGE', 'DOCUMENT_REQUEST_FAILURE_COUNT', 'DOCUMENT_REQUEST_FAILURE_DENOMINATOR', 'DOCUMENT_REQUEST_FAILURE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['document_request_failure'],
                    'order': 15
                },
                'threatening': {
                    'function': _get_metric_func('calculate_threatening_percentage'),
                    'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['threatening'],
                    'order': 9
                },
                'client_suspecting_ai': {
                    'function': _get_metric_func('calculate_client_suspecting_ai_percentage'),
                    'columns': ['MAID_SUSPECTING_AI_COUNT', 'MAID_SUSPECTING_AI_PERCENTAGE', 'MAID_SUSPECTING_AI_DENOMINATOR', 'MAID_SUSPECTING_AI_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['client_suspecting_ai'],
                    'order': 14
                },
                # 'threatening_case_identifier': {
                #     'function': _get_metric_func('calculate_threatening_case_identifier_percentage'),
                #     'columns': ['THREATENING_CASE_IDENTIFIER_PERCENTAGE', 'THREATENING_CASE_IDENTIFIER_COUNT', 'THREATENING_CASE_IDENTIFIER_DENOMINATOR', 'THREATENING_CASE_IDENTIFIER_SUMMARY_SUCCESS', 'THREATENING_CASE_IDENTIFIER_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['threatening_case_identifier'],
                #     'order': 9
                # },
                # Adding 8 new metrics
                # 'false_promises': {
                #     'function': _get_metric_func('calculate_false_promises_percentage'),
                #     'columns': ['FALSE_PROMISES_PERCENTAGE', 'FALSE_PROMISES_COUNT', 'FALSE_PROMISES_DENOMINATOR', 'FALSE_PROMISES_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['false_promises'],
                #     'order': 8
                # },
                # 'legal_metrics': {
                #     'function': _get_metric_func('calculate_legal_metrics'),
                #     'columns': ['ESCALATION_RATE', 'ESCALATION_COUNT', 'ESCALATION_DENOMINATOR', 'LEGAL_CONCERNS_PERCENTAGE', 'LEGAL_CONCERNS_COUNT', 'LEGAL_CONCERNS_DENOMINATOR', 'LEGAL_ALIGNMENT_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['legal_alignment'],
                #     'order': 10
                # },
                # 'call_request_metrics': {
                #     'function': _get_metric_func('calculate_call_request_metrics'),
                #     'columns': ['CALL_REQUEST_RATE', 'CALL_REQUEST_COUNT', 'CALL_REQUEST_DENOMINATOR', 'CALL_REQUEST_ANALYSIS_SUMMARY', 'REBUTTAL_RESULT_RATE', 'REBUTTAL_RESULT_DENOMINATOR', 'NO_RETENTION_COUNT'],
                #     'depends_on_prompts': ['call_request'],
                #     'order': 11
                # },
                # 'intervention_transfer': {
                #     'function': _get_metric_func('calculate_overall_percentages'),
                #     'columns': ['INTERVENTION_PERCENTAGE', 'INTERVENTION_COUNT', 'INTERVENTION_DENOMINATOR', 'TRANSFER_PERCENTAGE', 'TRANSFER_COUNT', 'TRANSFER_DENOMINATOR', 'INTERVENTION_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['categorizing'],
                #     'order': 12
                # },
                'ftr': {
                    'function': _get_metric_func('calculate_ftr_percentage'),
                    'columns': ['FTR_PERCENTAGE', 'FTR_COUNT', 'FTR_DENOMINATOR', 'FTR_REACHED_OUT_2_TIMES', 'FTR_REACHED_OUT_3_TIMES', 'FTR_REACHED_OUT_MORE_THAN_3_TIMES', 'FTR_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['ftr'],
                    'order': 13
                },
                # 'policy_escalation': {
                #     'function': _get_metric_func('calculate_policy_escalation_percentage'),
                #     'columns': ['POLICY_ESCALATION_PERCENTAGE', 'POLICY_ESCALATION_COUNT', 'POLICY_ESCALATION_DENOMINATOR', 'POLICY_ESCALATION_ANALYSIS_SUMMARY', 'TRANSFER_DUE_TO_ESCALATION_PERCENTAGE', 'TRANSFER_DUE_TO_ESCALATION_COUNT', 'TRANSFER_DUE_TO_ESCALATION_DENOMINATOR'],
                #     'depends_on_prompts': ['policy_escalation'],
                #     'order': 14
                # },
                # 'unsatisfactory_policy': {
                #     'function': _get_metric_func('calculate_unsatisfactory_policy_percentage'),
                #     'columns': ['UNSATISFACTORY_POLICY_PERCENTAGE', 'UNSATISFACTORY_POLICY_COUNT', 'UNSATISFACTORY_POLICY_DENOMINATOR', 'UNSATISFACTORY_POLICY_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['unsatisfactory_policy'],
                #     'order': 15
                # },
            }
        },
        'AT_Filipina': {
            'master_table': 'AT_FILIPINA_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                'loss_interest': {
                    'function': create_loss_interest_summary_report,
                    'columns': ['LOSS_INTEREST_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['loss_interest'],
                    'order': 2
                },
                'policy_violation': {
                    'function': _get_metric_func('calculate_at_african_policy_violation_metrics'),  # Reuse AT_African function
                    'columns': [
                        'MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT',
                        'WRONG_POLICY_PERCENTAGE', 'WRONG_POLICY_COUNT',
                        'FRUSTRATION_PERCENTAGE', 'FRUSTRATION_COUNT',
                        'LACK_OF_CLARITY_PERCENTAGE', 'LACK_OF_CLARITY_COUNT',
                        'POLICY_DENOMINATOR', 'POLICY_VIOLATION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['policy_violation'],
                    'order': 3
                },
                'tool': {
                    'function': _get_metric_func('calculate_at_african_tool_metrics'),  # Reuse AT_African function
                    'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'MISSING_TOOL_PERCENTAGE', 'MISSING_TOOL_COUNT', 'TOOL_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['tool'],
                    'order': 4
                },
                # 'tool': {
                #     'function': _get_metric_func('generate_at_filipina_tool_summary_report'),
                #     'columns': ['WRONG_TOOL_PERCENTAGE', 'FALSE_TRIGGER_COUNT', 'MISSING_TOOL_PERCENTAGE', 'MISSING_TRIGGER_COUNT', 'TOOL_ANALYSIS_SUMMARY', 'TOOL_SUMMARY_SUCCESS'],
                #     'depends_on_prompts': ['tool'],
                #     'order': 4
                # },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 5
                },
                'grammar': {
                    'function': _get_metric_func('calculate_grammar_metrics'),
                    'columns': ['GRAMMAR_ISSUE_PERCENTAGE', 'GRAMMAR_ISSUE_COUNT', 'GRAMMAR_ISSUE_DENOMINATOR', 'GRAMMAR_ISSUE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['grammar'],
                    'order': 6
                },
                'legitimacy': {
                    'function': _get_metric_func('calculate_legitimacy_metrics'),
                    'columns': ['LEGITIMACY_PERCENTAGE', 'LEGITIMACY_COUNT', 'LEGITIMACY_DENOMINATOR', 'LEGITIMACY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['legitimacy'],
                    'order': 7
                },
                'repetition': {
                    'function': _get_metric_func('calculate_repetition_metrics'),
                    'columns': [
                        'EXACT_REPETITION_PERCENTAGE', 'EXACT_REPETITION_COUNT', 'EXACT_REPETITION_DENOMINATOR', 'EXACT_REPETITION_ANALYSIS_SUMMARY',
                        'CONTEXTUAL_REPETITION_PERCENTAGE', 'CONTEXTUAL_REPETITION_COUNT', 'CONTEXTUAL_REPETITION_DENOMINATOR', 'CONTEXTUAL_REPETITION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['repetition'],
                    'order': 8
                },
                'backed_out': {
                    'function': _get_metric_func('calculate_backed_out_metrics'),
                    'columns': [
                        'BACKED_OUT_FOUND_ANOTHER_JOB_PERCENTAGE',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_COUNT',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_DENOMINATOR',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_ANALYSIS_SUMMARY',
                        'BACKED_OUT_REQUESTED_CANCELLATION_PERCENTAGE',
                        'BACKED_OUT_REQUESTED_CANCELLATION_COUNT',
                        'BACKED_OUT_REQUESTED_CANCELLATION_DENOMINATOR',
                        'BACKED_OUT_REQUESTED_CANCELLATION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['backed_out'],
                    'order': 9
                },
                'closing_message': {
                    'function': _get_metric_func('calculate_closing_message_metrics'),
                    'columns': [
                        'MISSING_CLOSING_MESSAGE_PERCENTAGE',
                        'MISSING_CLOSING_MESSAGE_COUNT',
                        'MISSING_CLOSING_MESSAGE_DENOMINATOR',
                        'MISSING_CLOSING_MESSAGE_ANALYSIS_SUMMARY',
                        'INCORRECT_CLOSING_MESSAGE_PERCENTAGE',
                        'INCORRECT_CLOSING_MESSAGE_COUNT',
                        'INCORRECT_CLOSING_MESSAGE_DENOMINATOR',
                        'INCORRECT_CLOSING_MESSAGE_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['closing_message'],
                    'order': 10
                },
                'negative_tool_response': {
                    'function': _get_metric_func('calculate_negative_tool_response_metrics'),
                    'columns': ['NEGATIVE_TOOL_RESPONSE_PERCENTAGE', 'NEGATIVE_TOOL_RESPONSE_COUNT', 
                                'NEGATIVE_TOOL_RESPONSE_DENOMINATOR', 'NEGATIVE_TOOL_RESPONSE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['negative_tool_response'],
                    'order': 11
                },
                'threatening': {
                    'function': _get_metric_func('calculate_threatening_percentage'),
                    'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['threatening'],
                    'order': 12
                },
                'loss_interest_7DMA': {
                    'function': create_loss_interest_summary_report_7DMA,
                    'columns': ['LOSS_INTEREST_7DMA_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['loss_interest'],
                    'order': 13
                },
                'flow_order': {
                    'function': _get_metric_func('calculate_kenyan_flow_order_metrics'),
                    'columns': ['INCORRECT_FLOW_ORDER_PERCENTAGE', 'INCORRECT_FLOW_ORDER_COUNT', 
                                'FLOW_ORDER_DENOMINATOR', 'FLOW_ORDER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['flow_order'],
                    'order': 14
                },
                'profile_update': {
                    'function': _get_metric_func('calculate_kenyan_profile_update_metrics'),
                    'columns': ['INCORRECT_PROFILE_UPDATE_PERCENTAGE', 'INCORRECT_PROFILE_UPDATE_COUNT', 
                                'PROFILE_UPDATE_DENOMINATOR', 'PROFILE_UPDATE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['profile_update'],
                    'order': 15
                },
            }
        },
        'AT_Filipina_In_PHL': {
            'master_table': 'AT_FILIPINA_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                'loss_interest': {
                    'function': create_loss_interest_summary_report,
                    'columns': ['LOSS_INTEREST_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['loss_interest'],
                    'order': 2
                },
                'policy_violation': {
                    'function': _get_metric_func('calculate_at_african_policy_violation_metrics'),  # Reuse AT_African function
                    'columns': [
                        'MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT',
                        'WRONG_POLICY_PERCENTAGE', 'WRONG_POLICY_COUNT',
                        'FRUSTRATION_PERCENTAGE', 'FRUSTRATION_COUNT',
                        'LACK_OF_CLARITY_PERCENTAGE', 'LACK_OF_CLARITY_COUNT',
                        'POLICY_DENOMINATOR', 'POLICY_VIOLATION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['policy_violation'],
                    'order': 3
                },
                'tool': {
                    'function': _get_metric_func('calculate_at_african_tool_metrics'),  # Reuse AT_African function
                    'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'MISSING_TOOL_PERCENTAGE', 'MISSING_TOOL_COUNT', 'TOOL_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['tool'],
                    'order': 4
                },
                # 'tool': {
                #     'function': _get_metric_func('generate_at_filipina_tool_summary_report'),
                #     'columns': ['WRONG_TOOL_PERCENTAGE', 'FALSE_TRIGGER_COUNT', 'MISSING_TOOL_PERCENTAGE', 'MISSING_TRIGGER_COUNT', 'TOOL_ANALYSIS_SUMMARY', 'TOOL_SUMMARY_SUCCESS'],
                #     'depends_on_prompts': ['tool'],
                #     'order': 4
                # },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 5
                },
                'grammar': {
                    'function': _get_metric_func('calculate_grammar_metrics'),
                    'columns': ['GRAMMAR_ISSUE_PERCENTAGE', 'GRAMMAR_ISSUE_COUNT', 'GRAMMAR_ISSUE_DENOMINATOR', 'GRAMMAR_ISSUE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['grammar'],
                    'order': 6
                },
                'legitimacy': {
                    'function': _get_metric_func('calculate_legitimacy_metrics'),
                    'columns': ['LEGITIMACY_PERCENTAGE', 'LEGITIMACY_COUNT', 'LEGITIMACY_DENOMINATOR', 'LEGITIMACY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['legitimacy'],
                    'order': 7
                },
                'repetition': {
                    'function': _get_metric_func('calculate_repetition_metrics'),
                    'columns': [
                        'EXACT_REPETITION_PERCENTAGE', 'EXACT_REPETITION_COUNT', 'EXACT_REPETITION_DENOMINATOR', 'EXACT_REPETITION_ANALYSIS_SUMMARY',
                        'CONTEXTUAL_REPETITION_PERCENTAGE', 'CONTEXTUAL_REPETITION_COUNT', 'CONTEXTUAL_REPETITION_DENOMINATOR', 'CONTEXTUAL_REPETITION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['repetition'],
                    'order': 8
                },
                'backed_out': {
                    'function': _get_metric_func('calculate_backed_out_metrics'),
                    'columns': [
                        'BACKED_OUT_FOUND_ANOTHER_JOB_PERCENTAGE',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_COUNT',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_DENOMINATOR',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_ANALYSIS_SUMMARY',
                        'BACKED_OUT_REQUESTED_CANCELLATION_PERCENTAGE',
                        'BACKED_OUT_REQUESTED_CANCELLATION_COUNT',
                        'BACKED_OUT_REQUESTED_CANCELLATION_DENOMINATOR',
                        'BACKED_OUT_REQUESTED_CANCELLATION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['backed_out'],
                    'order': 9
                },
                'closing_message': {
                    'function': _get_metric_func('calculate_closing_message_metrics'),
                    'columns': [
                        'MISSING_CLOSING_MESSAGE_PERCENTAGE',
                        'MISSING_CLOSING_MESSAGE_COUNT',
                        'MISSING_CLOSING_MESSAGE_DENOMINATOR',
                        'MISSING_CLOSING_MESSAGE_ANALYSIS_SUMMARY',
                        'INCORRECT_CLOSING_MESSAGE_PERCENTAGE',
                        'INCORRECT_CLOSING_MESSAGE_COUNT',
                        'INCORRECT_CLOSING_MESSAGE_DENOMINATOR',
                        'INCORRECT_CLOSING_MESSAGE_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['closing_message'],
                    'order': 10
                },
                'negative_tool_response': {
                    'function': _get_metric_func('calculate_negative_tool_response_metrics'),
                    'columns': ['NEGATIVE_TOOL_RESPONSE_PERCENTAGE', 'NEGATIVE_TOOL_RESPONSE_COUNT', 
                                'NEGATIVE_TOOL_RESPONSE_DENOMINATOR', 'NEGATIVE_TOOL_RESPONSE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['negative_tool_response'],
                    'order': 11
                },
                # 'threatening': {
                #     'function': _get_metric_func('calculate_threatening_percentage'),
                #     'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['threatening'],
                #     'order': 12
                # },
                'loss_interest_7DMA': {
                    'function': create_loss_interest_summary_report_7DMA,
                    'columns': ['LOSS_INTEREST_7DMA_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['loss_interest'],
                    'order': 13
                },
                'flow_order': {
                    'function': _get_metric_func('calculate_kenyan_flow_order_metrics'),
                    'columns': ['INCORRECT_FLOW_ORDER_PERCENTAGE', 'INCORRECT_FLOW_ORDER_COUNT', 
                                'FLOW_ORDER_DENOMINATOR', 'FLOW_ORDER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['flow_order'],
                    'order': 14
                },
                'profile_update': {
                    'function': _get_metric_func('calculate_kenyan_profile_update_metrics'),
                    'columns': ['INCORRECT_PROFILE_UPDATE_PERCENTAGE', 'INCORRECT_PROFILE_UPDATE_COUNT', 
                                'PROFILE_UPDATE_DENOMINATOR', 'PROFILE_UPDATE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['profile_update'],
                    'order': 15
                },
            }
        },
        'AT_Filipina_Outside_UAE': {
            'master_table': 'AT_FILIPINA_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                'loss_interest': {
                    'function': create_loss_interest_summary_report,
                    'columns': ['LOSS_INTEREST_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['loss_interest'],
                    'order': 2
                },
                'policy_violation': {
                    'function': _get_metric_func('calculate_at_african_policy_violation_metrics'),  # Reuse AT_African function
                    'columns': [
                        'MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT',
                        'WRONG_POLICY_PERCENTAGE', 'WRONG_POLICY_COUNT',
                        'FRUSTRATION_PERCENTAGE', 'FRUSTRATION_COUNT',
                        'LACK_OF_CLARITY_PERCENTAGE', 'LACK_OF_CLARITY_COUNT',
                        'POLICY_DENOMINATOR', 'POLICY_VIOLATION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['policy_violation'],
                    'order': 3
                },
                'tool': {
                    'function': _get_metric_func('calculate_at_african_tool_metrics'),  # Reuse AT_African function
                    'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'MISSING_TOOL_PERCENTAGE', 'MISSING_TOOL_COUNT', 'TOOL_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['tool'],
                    'order': 4
                },
                # 'tool': {
                #     'function': _get_metric_func('generate_at_filipina_tool_summary_report'),
                #     'columns': ['WRONG_TOOL_PERCENTAGE', 'FALSE_TRIGGER_COUNT', 'MISSING_TOOL_PERCENTAGE', 'MISSING_TRIGGER_COUNT', 'TOOL_ANALYSIS_SUMMARY', 'TOOL_SUMMARY_SUCCESS'],
                #     'depends_on_prompts': ['tool'],
                #     'order': 4
                # },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 5
                },
                'grammar': {
                    'function': _get_metric_func('calculate_grammar_metrics'),
                    'columns': ['GRAMMAR_ISSUE_PERCENTAGE', 'GRAMMAR_ISSUE_COUNT', 'GRAMMAR_ISSUE_DENOMINATOR', 'GRAMMAR_ISSUE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['grammar'],
                    'order': 6
                },
                'legitimacy': {
                    'function': _get_metric_func('calculate_legitimacy_metrics'),
                    'columns': ['LEGITIMACY_PERCENTAGE', 'LEGITIMACY_COUNT', 'LEGITIMACY_DENOMINATOR', 'LEGITIMACY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['legitimacy'],
                    'order': 7
                },
                'repetition': {
                    'function': _get_metric_func('calculate_repetition_metrics'),
                    'columns': [
                        'EXACT_REPETITION_PERCENTAGE', 'EXACT_REPETITION_COUNT', 'EXACT_REPETITION_DENOMINATOR', 'EXACT_REPETITION_ANALYSIS_SUMMARY',
                        'CONTEXTUAL_REPETITION_PERCENTAGE', 'CONTEXTUAL_REPETITION_COUNT', 'CONTEXTUAL_REPETITION_DENOMINATOR', 'CONTEXTUAL_REPETITION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['repetition'],
                    'order': 8
                },
                'backed_out': {
                    'function': _get_metric_func('calculate_backed_out_metrics'),
                    'columns': [
                        'BACKED_OUT_FOUND_ANOTHER_JOB_PERCENTAGE',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_COUNT',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_DENOMINATOR',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_ANALYSIS_SUMMARY',
                        'BACKED_OUT_REQUESTED_CANCELLATION_PERCENTAGE',
                        'BACKED_OUT_REQUESTED_CANCELLATION_COUNT',
                        'BACKED_OUT_REQUESTED_CANCELLATION_DENOMINATOR',
                        'BACKED_OUT_REQUESTED_CANCELLATION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['backed_out'],
                    'order': 9
                },
                'closing_message': {
                    'function': _get_metric_func('calculate_closing_message_metrics'),
                    'columns': [
                        'MISSING_CLOSING_MESSAGE_PERCENTAGE',
                        'MISSING_CLOSING_MESSAGE_COUNT',
                        'MISSING_CLOSING_MESSAGE_DENOMINATOR',
                        'MISSING_CLOSING_MESSAGE_ANALYSIS_SUMMARY',
                        'INCORRECT_CLOSING_MESSAGE_PERCENTAGE',
                        'INCORRECT_CLOSING_MESSAGE_COUNT',
                        'INCORRECT_CLOSING_MESSAGE_DENOMINATOR',
                        'INCORRECT_CLOSING_MESSAGE_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['closing_message'],
                    'order': 10
                },
                'negative_tool_response': {
                    'function': _get_metric_func('calculate_negative_tool_response_metrics'),
                    'columns': ['NEGATIVE_TOOL_RESPONSE_PERCENTAGE', 'NEGATIVE_TOOL_RESPONSE_COUNT', 
                                'NEGATIVE_TOOL_RESPONSE_DENOMINATOR', 'NEGATIVE_TOOL_RESPONSE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['negative_tool_response'],
                    'order': 11
                },
                'threatening': {
                    'function': _get_metric_func('calculate_threatening_percentage'),
                    'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['threatening'],
                    'order': 12
                },
                'loss_interest_7DMA': {
                    'function': create_loss_interest_summary_report_7DMA,
                    'columns': ['LOSS_INTEREST_7DMA_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['loss_interest'],
                    'order': 13
                },
                'flow_order': {
                    'function': _get_metric_func('calculate_kenyan_flow_order_metrics'),
                    'columns': ['INCORRECT_FLOW_ORDER_PERCENTAGE', 'INCORRECT_FLOW_ORDER_COUNT', 
                                'FLOW_ORDER_DENOMINATOR', 'FLOW_ORDER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['flow_order'],
                    'order': 14
                },
                'profile_update': {
                    'function': _get_metric_func('calculate_kenyan_profile_update_metrics'),
                    'columns': ['INCORRECT_PROFILE_UPDATE_PERCENTAGE', 'INCORRECT_PROFILE_UPDATE_COUNT', 
                                'PROFILE_UPDATE_DENOMINATOR', 'PROFILE_UPDATE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['profile_update'],
                    'order': 15
                },
            }
        },
        'AT_Filipina_Inside_UAE': {
            'master_table': 'AT_FILIPINA_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                'policy_violation': {
                    'function': _get_metric_func('calculate_at_african_policy_violation_metrics'),  # Reuse AT_African function
                    'columns': [
                        'MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT',
                        'WRONG_POLICY_PERCENTAGE', 'WRONG_POLICY_COUNT',
                        'FRUSTRATION_PERCENTAGE', 'FRUSTRATION_COUNT',
                        'LACK_OF_CLARITY_PERCENTAGE', 'LACK_OF_CLARITY_COUNT',
                        'POLICY_DENOMINATOR', 'POLICY_VIOLATION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['policy_violation'],
                    'order': 2
                },
                # 'tool': {
                #     'function': _get_metric_func('generate_at_filipina_tool_summary_report'),
                #     'columns': ['WRONG_TOOL_PERCENTAGE', 'FALSE_TRIGGER_COUNT', 'MISSING_TOOL_PERCENTAGE', 'MISSING_TRIGGER_COUNT', 'TOOL_ANALYSIS_SUMMARY', 'TOOL_SUMMARY_SUCCESS'],
                #     'depends_on_prompts': ['tool'],
                #     'order': 3
                # },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 4
                },
                'grammar': {
                    'function': _get_metric_func('calculate_grammar_metrics'),
                    'columns': ['GRAMMAR_ISSUE_PERCENTAGE', 'GRAMMAR_ISSUE_COUNT', 'GRAMMAR_ISSUE_DENOMINATOR', 'GRAMMAR_ISSUE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['grammar'],
                    'order': 5
                },
                'legitimacy': {
                    'function': _get_metric_func('calculate_legitimacy_metrics'),
                    'columns': ['LEGITIMACY_PERCENTAGE', 'LEGITIMACY_COUNT', 'LEGITIMACY_DENOMINATOR', 'LEGITIMACY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['legitimacy'],
                    'order': 6
                },
                'repetition': {
                    'function': _get_metric_func('calculate_repetition_metrics'),
                    'columns': [
                        'EXACT_REPETITION_PERCENTAGE', 'EXACT_REPETITION_COUNT', 'EXACT_REPETITION_DENOMINATOR', 'EXACT_REPETITION_ANALYSIS_SUMMARY',
                        'CONTEXTUAL_REPETITION_PERCENTAGE', 'CONTEXTUAL_REPETITION_COUNT', 'CONTEXTUAL_REPETITION_DENOMINATOR', 'CONTEXTUAL_REPETITION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['repetition'],
                    'order': 7
                },
                'backed_out': {
                    'function': _get_metric_func('calculate_backed_out_metrics'),
                    'columns': [
                        'BACKED_OUT_FOUND_ANOTHER_JOB_PERCENTAGE',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_COUNT',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_DENOMINATOR',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_ANALYSIS_SUMMARY',
                        'BACKED_OUT_REQUESTED_CANCELLATION_PERCENTAGE',
                        'BACKED_OUT_REQUESTED_CANCELLATION_COUNT',
                        'BACKED_OUT_REQUESTED_CANCELLATION_DENOMINATOR',
                        'BACKED_OUT_REQUESTED_CANCELLATION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['backed_out'],
                    'order': 8
                },
                'closing_message': {
                    'function': _get_metric_func('calculate_closing_message_metrics'),
                    'columns': [
                        'MISSING_CLOSING_MESSAGE_PERCENTAGE',
                        'MISSING_CLOSING_MESSAGE_COUNT',
                        'MISSING_CLOSING_MESSAGE_DENOMINATOR',
                        'MISSING_CLOSING_MESSAGE_ANALYSIS_SUMMARY',
                        'INCORRECT_CLOSING_MESSAGE_PERCENTAGE',
                        'INCORRECT_CLOSING_MESSAGE_COUNT',
                        'INCORRECT_CLOSING_MESSAGE_DENOMINATOR',
                        'INCORRECT_CLOSING_MESSAGE_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['closing_message'],
                    'order': 9
                },
                'negative_tool_response': {
                    'function': _get_metric_func('calculate_negative_tool_response_metrics'),
                    'columns': ['NEGATIVE_TOOL_RESPONSE_PERCENTAGE', 'NEGATIVE_TOOL_RESPONSE_COUNT', 
                                'NEGATIVE_TOOL_RESPONSE_DENOMINATOR', 'NEGATIVE_TOOL_RESPONSE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['negative_tool_response'],
                    'order': 10
                },
                'threatening': {
                    'function': _get_metric_func('calculate_threatening_percentage'),
                    'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['threatening'],
                    'order': 11
                },
                'flow_order': {
                    'function': _get_metric_func('calculate_kenyan_flow_order_metrics'),
                    'columns': ['INCORRECT_FLOW_ORDER_PERCENTAGE', 'INCORRECT_FLOW_ORDER_COUNT', 
                                'FLOW_ORDER_DENOMINATOR', 'FLOW_ORDER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['flow_order'],
                    'order': 12
                },
                'profile_update': {
                    'function': _get_metric_func('calculate_kenyan_profile_update_metrics'),
                    'columns': ['INCORRECT_PROFILE_UPDATE_PERCENTAGE', 'INCORRECT_PROFILE_UPDATE_COUNT', 
                                'PROFILE_UPDATE_DENOMINATOR', 'PROFILE_UPDATE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['profile_update'],
                    'order': 13
                },
            }
        },
        'AT_African': {
            'master_table': 'AT_AFRICAN_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 2
                },
                'loss_interest': {
                    'function': create_loss_interest_summary_report_african_ethiopian,
                    'columns': ['LOSS_INTEREST_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['loss_interest'],
                    'order': 3
                },
                'transfer': {
                    'function': _get_metric_func('calculate_at_african_transfer_metrics'),
                    'columns': ['TRANSFER_ESCALATION_PERCENTAGE', 'TRANSFER_ESCALATION_COUNT', 'TRANSFER_KNOWN_FLOW_PERCENTAGE', 'TRANSFER_KNOWN_FLOW_COUNT', 'TRANSFER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['transfer'],
                    'order': 4
                },
                'policy_violation': {
                    'function': _get_metric_func('calculate_at_african_policy_violation_metrics'),
                    'columns': [
                        'MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT',
                        'WRONG_POLICY_PERCENTAGE', 'WRONG_POLICY_COUNT',
                        'FRUSTRATION_PERCENTAGE', 'FRUSTRATION_COUNT',
                        'LACK_OF_CLARITY_PERCENTAGE', 'LACK_OF_CLARITY_COUNT',
                        'POLICY_DENOMINATOR', 'POLICY_VIOLATION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['policy_violation'],
                    'order': 5
                },
                'tool': {
                    'function': _get_metric_func('calculate_at_african_tool_metrics'),
                    'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'MISSING_TOOL_PERCENTAGE', 'MISSING_TOOL_COUNT', 'TOOL_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['tool'],
                    'order': 6
                },
                'grammar': {
                    'function': _get_metric_func('calculate_grammar_metrics'),
                    'columns': ['GRAMMAR_ISSUE_PERCENTAGE', 'GRAMMAR_ISSUE_COUNT', 'GRAMMAR_ISSUE_DENOMINATOR', 'GRAMMAR_ISSUE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['grammar'],
                    'order': 7
                },
                'legitimacy': {
                    'function': _get_metric_func('calculate_legitimacy_metrics'),
                    'columns': ['LEGITIMACY_PERCENTAGE', 'LEGITIMACY_COUNT', 'LEGITIMACY_DENOMINATOR', 'LEGITIMACY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['legitimacy'],
                    'order': 8
                },
                'repetition': {
                    'function': _get_metric_func('calculate_repetition_metrics'),
                    'columns': [
                        'EXACT_REPETITION_PERCENTAGE', 'EXACT_REPETITION_COUNT', 'EXACT_REPETITION_DENOMINATOR', 'EXACT_REPETITION_ANALYSIS_SUMMARY',
                        'CONTEXTUAL_REPETITION_PERCENTAGE', 'CONTEXTUAL_REPETITION_COUNT', 'CONTEXTUAL_REPETITION_DENOMINATOR', 'CONTEXTUAL_REPETITION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['repetition'],
                    'order': 9
                },
                'backed_out': {
                    'function': _get_metric_func('calculate_backed_out_metrics'),
                    'columns': [
                        'BACKED_OUT_FOUND_ANOTHER_JOB_PERCENTAGE',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_COUNT',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_DENOMINATOR',
                        'BACKED_OUT_FOUND_ANOTHER_JOB_ANALYSIS_SUMMARY',
                        'BACKED_OUT_REQUESTED_CANCELLATION_PERCENTAGE',
                        'BACKED_OUT_REQUESTED_CANCELLATION_COUNT',
                        'BACKED_OUT_REQUESTED_CANCELLATION_DENOMINATOR',
                        'BACKED_OUT_REQUESTED_CANCELLATION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['backed_out'],
                    'order': 10
                },
                'closing_message': {
                    'function': _get_metric_func('calculate_closing_message_metrics'),
                    'columns': [
                        'MISSING_CLOSING_MESSAGE_PERCENTAGE',
                        'MISSING_CLOSING_MESSAGE_COUNT',
                        'MISSING_CLOSING_MESSAGE_DENOMINATOR',
                        'MISSING_CLOSING_MESSAGE_ANALYSIS_SUMMARY',
                        'INCORRECT_CLOSING_MESSAGE_PERCENTAGE',
                        'INCORRECT_CLOSING_MESSAGE_COUNT',
                        'INCORRECT_CLOSING_MESSAGE_DENOMINATOR',
                        'INCORRECT_CLOSING_MESSAGE_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['closing_message'],
                    'order': 11
                },
                'threatening': {
                    'function': _get_metric_func('calculate_threatening_percentage'),
                    'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['threatening'],
                    'order': 12
                },
                'negative_tool_response': {
                    'function': _get_metric_func('calculate_negative_tool_response_metrics'),
                    'columns': ['NEGATIVE_TOOL_RESPONSE_PERCENTAGE', 'NEGATIVE_TOOL_RESPONSE_COUNT', 
                                'NEGATIVE_TOOL_RESPONSE_DENOMINATOR', 'NEGATIVE_TOOL_RESPONSE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['negative_tool_response'],
                    'order': 13
                },
                'flow_order': {
                    'function': _get_metric_func('calculate_kenyan_flow_order_metrics'),
                    'columns': ['INCORRECT_FLOW_ORDER_PERCENTAGE', 'INCORRECT_FLOW_ORDER_COUNT', 
                                'FLOW_ORDER_DENOMINATOR', 'FLOW_ORDER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['flow_order'],
                    'order': 14
                },
                'profile_update': {
                    'function': _get_metric_func('calculate_kenyan_profile_update_metrics'),
                    'columns': ['INCORRECT_PROFILE_UPDATE_PERCENTAGE', 'INCORRECT_PROFILE_UPDATE_COUNT', 
                                'PROFILE_UPDATE_DENOMINATOR', 'PROFILE_UPDATE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['profile_update'],
                    'order': 15
                },
                'tool_eval': {
                        'function': _get_metric_func('calculate_tool_eval_metrics'),
                        'columns': ['TOOL_EVAL_WRONG_PCT', 'TOOL_EVAL_MISSING_PCT', 'TOOL_EVAL_TOTAL_MESSAGES', 'TOOL_EVAL_SUMMARY_SUCCESS', 'TOOL_EVAL_ANALYSIS_SUMMARY'],
                        'depends_on_prompts': ['tool_eval'],
                        'order': 16
                    },
                # 'incorrect_assessment_1': {
                #     'function': _get_metric_func('calculate_incorrect_assessment_1_metrics'),
                #     'columns': [
                #         'COUNTRY_INCORRECT_PERCENTAGE', 'COUNTRY_INCORRECT_COUNT',
                #         'YEARS_INCORRECT_PERCENTAGE', 'YEARS_INCORRECT_COUNT',
                #         'AGE_INCORRECT_PERCENTAGE', 'AGE_INCORRECT_COUNT',
                #         'CHILDREN_INCORRECT_PERCENTAGE', 'CHILDREN_INCORRECT_COUNT',
                #         'CLIENT_SCENARIO_INCORRECT_PERCENTAGE', 'CLIENT_SCENARIO_INCORRECT_COUNT',
                #         'INCORRECT_ASSESSMENT_1_DENOMINATOR', 'INCORRECT_ASSESSMENT_1_ANALYSIS_SUMMARY'
                #     ],
                #     'depends_on_prompts': ['incorrect_assessment_1'],
                #     'order': 17
                # },
                # 'incorrect_assessment_2': {
                #     'function': _get_metric_func('calculate_incorrect_assessment_2_metrics'),
                #     'columns': [
                #         'HEIGHT_INCORRECT_PERCENTAGE', 'HEIGHT_INCORRECT_COUNT',
                #         'BMI_INCORRECT_PERCENTAGE', 'BMI_INCORRECT_COUNT',
                #         'INCORRECT_ASSESSMENT_2_DENOMINATOR', 'INCORRECT_ASSESSMENT_2_ANALYSIS_SUMMARY'
                #     ],
                #     'depends_on_prompts': ['incorrect_assessment_2'],
                #     'order': 18
                # },
                # 'poke_check': {
                #     'function': _get_metric_func('calculate_poke_check_metrics'),
                #     'columns': [
                #         'MISSING_CLOSING_MESSAGE_PERCENTAGE', 'MISSING_CLOSING_MESSAGE_COUNT',
                #         'INCORRECT_CLOSING_MESSAGE_PERCENTAGE', 'INCORRECT_CLOSING_MESSAGE_COUNT',
                #         'MISSING_CLIENT_SCENARIO_PERCENTAGE', 'MISSING_CLIENT_SCENARIO_COUNT',
                #         'POKE_CHECK_DENOMINATOR', 'POKE_CHECK_ANALYSIS_SUMMARY'
                #     ],
                #     'depends_on_prompts': ['poke_check'],
                #     'order': 19
                # },
                # 'facephoto_analysis': {
                #     'function': _get_metric_func('calculate_facephoto_analysis_metrics'),
                #     'columns': [
                #         'MISSING_VISION_PROMPT_RESPONSE_PERCENTAGE', 'MISSING_VISION_PROMPT_RESPONSE_COUNT',
                #         'INCORRECT_VISION_PROMPT_RESPONSE_PERCENTAGE', 'INCORRECT_VISION_PROMPT_RESPONSE_COUNT',
                #         'MISSED_ATTACHMENT_PERCENTAGE', 'MISSED_ATTACHMENT_COUNT',
                #         'FALSE_ATTACHMENT_PERCENTAGE', 'FALSE_ATTACHMENT_COUNT',
                #         'SAMPLE_PHOTO_NOT_SENT_PERCENTAGE', 'SAMPLE_PHOTO_NOT_SENT_COUNT',
                #         'ATTACHMENT_TOOL_FAILURE_PERCENTAGE', 'ATTACHMENT_TOOL_FAILURE_COUNT',
                #         'FACEPHOTO_ANALYSIS_DENOMINATOR', 'FACEPHOTO_ANALYSIS_SUMMARY'
                #     ],
                #     'depends_on_prompts': ['facephoto_analysis'],
                #     'order': 20
                # },
                # 'passport_analysis': {
                #     'function': _get_metric_func('calculate_passport_analysis_metrics'),
                #     'columns': [
                #         'PASSPORT_MISSING_VISION_PROMPT_RESPONSE_PERCENTAGE', 'PASSPORT_MISSING_VISION_PROMPT_RESPONSE_COUNT',
                #         'PASSPORT_INCORRECT_VISION_PROMPT_RESPONSE_PERCENTAGE', 'PASSPORT_INCORRECT_VISION_PROMPT_RESPONSE_COUNT',
                #         'PASSPORT_MISSED_ATTACHMENT_PERCENTAGE', 'PASSPORT_MISSED_ATTACHMENT_COUNT',
                #         'PASSPORT_FALSE_ATTACHMENT_PERCENTAGE', 'PASSPORT_FALSE_ATTACHMENT_COUNT',
                #         'PASSPORT_SAMPLE_PHOTO_NOT_SENT_PERCENTAGE', 'PASSPORT_SAMPLE_PHOTO_NOT_SENT_COUNT',
                #         'PASSPORT_ATTACHMENT_TOOL_FAILURE_PERCENTAGE', 'PASSPORT_ATTACHMENT_TOOL_FAILURE_COUNT',
                #         'PASSPORT_ANALYSIS_DENOMINATOR', 'PASSPORT_ANALYSIS_SUMMARY'
                #     ],
                #     'depends_on_prompts': ['passport_analysis'],
                #     'order': 21
                # },
                # 'mfa_analysis': {
                #     'function': _get_metric_func('calculate_mfa_analysis_metrics'),
                #     'columns': [
                #         'MFA_MISSING_VISION_PROMPT_RESPONSE_PERCENTAGE', 'MFA_MISSING_VISION_PROMPT_RESPONSE_COUNT',
                #         'MFA_INCORRECT_VISION_PROMPT_RESPONSE_PERCENTAGE', 'MFA_INCORRECT_VISION_PROMPT_RESPONSE_COUNT',
                #         'MFA_MISSED_ATTACHMENT_PERCENTAGE', 'MFA_MISSED_ATTACHMENT_COUNT',
                #         'MFA_FALSE_ATTACHMENT_PERCENTAGE', 'MFA_FALSE_ATTACHMENT_COUNT',
                #         'MFA_SAMPLE_PHOTO_NOT_SENT_PERCENTAGE', 'MFA_SAMPLE_PHOTO_NOT_SENT_COUNT',
                #         'MFA_ATTACHMENT_TOOL_FAILURE_PERCENTAGE', 'MFA_ATTACHMENT_TOOL_FAILURE_COUNT',
                #         'MFA_ANALYSIS_DENOMINATOR', 'MFA_ANALYSIS_SUMMARY'
                #     ],
                #     'depends_on_prompts': ['mfa_analysis'],
                #     'order': 22
                # },
                # 'gcc_analysis': {
                #     'function': _get_metric_func('calculate_gcc_analysis_metrics'),
                #     'columns': [
                #         'GCC_MISSING_VISION_PROMPT_RESPONSE_PERCENTAGE', 'GCC_MISSING_VISION_PROMPT_RESPONSE_COUNT',
                #         'GCC_INCORRECT_VISION_PROMPT_RESPONSE_PERCENTAGE', 'GCC_INCORRECT_VISION_PROMPT_RESPONSE_COUNT',
                #         'GCC_MISSED_ATTACHMENT_PERCENTAGE', 'GCC_MISSED_ATTACHMENT_COUNT',
                #         'GCC_FALSE_ATTACHMENT_PERCENTAGE', 'GCC_FALSE_ATTACHMENT_COUNT',
                #         'GCC_SAMPLE_PHOTO_NOT_SENT_PERCENTAGE', 'GCC_SAMPLE_PHOTO_NOT_SENT_COUNT',
                #         'GCC_ATTACHMENT_TOOL_FAILURE_PERCENTAGE', 'GCC_ATTACHMENT_TOOL_FAILURE_COUNT',
                #         'GCC_ANALYSIS_DENOMINATOR', 'GCC_ANALYSIS_SUMMARY'
                #     ],
                #     'depends_on_prompts': ['gcc_analysis'],
                #     'order': 23
                # },
                }
        },
        'Gulf_maids': {
            'master_table': 'GULF_MAIDS_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 2
                },
                'loss_interest': {
                    'function': create_gulf_maids_loss_interest_summary_report,
                    'columns': ['LOSS_INTEREST_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['loss_interest'],
                    'order': 3
                },
                'clarification': {
                    'function': _get_metric_func('calculate_gulf_maids_clarification_metrics'),
                    'columns': [
                        'CLARIFICATION_REQUESTED_PERCENTAGE',
                        'CLARIFICATION_REQUESTED_COUNT',
                        'CLARIFICATION_DENOMINATOR',
                        'AVG_CONSUMER_MESSAGES',
                        'AVG_CLARIFYING_QUESTIONS',
                        'CLARIFICATION_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['clarification'],
                    'order': 4
                },
                'tool': {
                    'function': _get_metric_func('calculate_gulf_maids_tool_metrics'),
                    'columns': [
                        'MISSING_TOOL_PERCENTAGE',
                        'MISSING_TOOL_COUNT',
                        'TOOL_DENOMINATOR',
                        'WRONG_TOOL_PERCENTAGE_UPDATE_PROFILE',
                        'WRONG_TOOL_COUNT_UPDATE_PROFILE',
                        'TOTAL_CALLS_UPDATE_PROFILE',
                        'WRONG_TOOL_PERCENTAGE_SAVE_PASSPORT',
                        'WRONG_TOOL_COUNT_SAVE_PASSPORT',
                        'TOTAL_CALLS_SAVE_PASSPORT',
                        'WRONG_TOOL_PERCENTAGE_TRANSFER_CHAT',
                        'WRONG_TOOL_COUNT_TRANSFER_CHAT',
                        'TOTAL_CALLS_TRANSFER_CHAT',
                        'WRONG_TOOL_PERCENTAGE_SEND_SAMPLE',
                        'WRONG_TOOL_COUNT_SEND_SAMPLE',
                        'TOTAL_CALLS_SEND_SAMPLE',
                        'TOOL_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['tool'],
                    'order': 5
                },
                # Task 37 — Broadcast Messages (CVR + Reaction analysis)
                'broadcast_messages': {
                    'function': create_broadcast_messages_summary_report,
                    'columns': ['BROADCAST_MESSAGES_SUMMARY_SUCCESS', 'BROADCAST_MESSAGES_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['broadcast_cvr', 'broadcast_reaction'],
                    'order': 6
                },
            }
        },
        # 'AT_Ethiopian': {
        #     'master_table': 'AT_ETHIOPIAN_SUMMARY',
        #     'metrics': {
        #         'weighted_nps': {
        #             'function': _get_metric_func('calculate_weighted_nps_per_department'),
        #             'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
        #             'depends_on_prompts': ['SA_prompt'],
        #             'order': 1
        #         },
        #         'total_chats': {
        #             'function': _get_metric_func('calculate_total_chats'),
        #             'columns': ['TOTAL_CHATS'],
        #             'depends_on_prompts': ['SA_prompt'],
        #             'order': 2
        #         },
        #         'loss_interest': {
        #             'function': create_loss_interest_summary_report_african_ethiopian,
        #             'columns': ['LOSS_INTEREST_SUMMARY_SUCCESS'],
        #             'depends_on_prompts': ['loss_interest'],
        #             'order': 3
        #         },
        #         'policy_violation': {
        #             'function': _get_metric_func('calculate_at_african_policy_violation_metrics'),
        #             'columns': [
        #                 'MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT',
        #                 'WRONG_POLICY_PERCENTAGE', 'WRONG_POLICY_COUNT',
        #                 'FRUSTRATION_PERCENTAGE', 'FRUSTRATION_COUNT',
        #                 'LACK_OF_CLARITY_PERCENTAGE', 'LACK_OF_CLARITY_COUNT',
        #                 'POLICY_DENOMINATOR', 'POLICY_VIOLATION_ANALYSIS_SUMMARY'
        #             ],
        #             'depends_on_prompts': ['policy_violation'],
        #             'order': 4
        #         },
        #         'tool': {
        #             'function': _get_metric_func('calculate_at_african_tool_metrics'),
        #             'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'MISSING_TOOL_PERCENTAGE', 'MISSING_TOOL_COUNT', 'TOOL_ANALYSIS_SUMMARY'],
        #             'depends_on_prompts': ['tool'],
        #             'order': 5
        #         },
        #         'transfer': {
        #             'function': _get_metric_func('calculate_at_african_transfer_metrics'),
        #             'columns': ['TRANSFER_ESCALATION_PERCENTAGE', 'TRANSFER_ESCALATION_COUNT', 'TRANSFER_KNOWN_FLOW_PERCENTAGE', 'TRANSFER_KNOWN_FLOW_COUNT', 'TRANSFER_ANALYSIS_SUMMARY'],
        #             'depends_on_prompts': ['transfer'],
        #             'order': 6
        #         },
        #         'legitimacy': {
        #             'function': _get_metric_func('calculate_legitimacy_metrics'),
        #             'columns': ['LEGITIMACY_PERCENTAGE', 'LEGITIMACY_COUNT', 'LEGITIMACY_DENOMINATOR', 'LEGITIMACY_ANALYSIS_SUMMARY'],
        #             'depends_on_prompts': ['legitimacy'],
        #             'order': 7
        #         },
        #         'threatening': {
        #             'function': _get_metric_func('calculate_threatening_percentage'),
        #             'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
        #             'depends_on_prompts': ['threatening'],
        #             'order': 8
        #         },
        #     }
        # },
        'CC_Sales': {
            'master_table': 'CC_SALES_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                # 'client_suspecting_ai': {
                #     'function': _get_metric_func('calculate_client_suspecting_ai_percentage'),
                #     'columns': ['CLIENT_SUSPECTING_AI_COUNT', 'CLIENT_SUSPECTING_AI_PERCENTAGE', 'CLIENT_SUSPECTING_AI_DENOMINATOR', 'CLIENT_SUSPECTING_AI_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['client_suspecting_ai'],
                #     'order': 2
                # },
                'clarity_score': {
                    'function': _get_metric_func('calculate_clarity_score_percentage'),
                    'columns': ['CLARITY_SCORE_PERCENTAGE', 'CLARITY_SCORE_COUNT', 'CLARITY_SCORE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['clarity_score'],
                    'order': 3
                },
                # 'sales_transfer': {
                #     'function': _get_metric_func('calculate_sales_transer_percentage'),
                #     'columns': ['TRANSFER_ESCALATION_PERCENTAGE_A', 'TRANSFER_ESCALATION_PERCENTAGE_B', 'TRANSFER_ESCALATION_COUNT', 'TRANSFER_KNOWN_FLOW_PERCENTAGE_A', 'TRANSFER_KNOWN_FLOW_PERCENTAGE_B', 'TRANSFER_KNOWN_FLOW_COUNT', 'TRANSFER_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['sales_transfer'],
                #     'order': 4
                # },
                'tool': {
                    'function': _get_metric_func('generate_tool_summary_report'),
                    'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'MISSING_TOOL_PERCENTAGE', 'MISSING_TOOL_COUNT', 'TOOL_ANALYSIS_SUMMARY', 'TOOL_SUMMARY_SUCCESS'],
                    'depends_on_prompts': ['tool'],
                    'order': 5
                },
                'policy_violation': {
                    'function': _get_metric_func('calculate_cc_sales_policy_violation_metrics'),
                    'columns': ['MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT', 'UNCLEAR_POLICY_PERCENTAGE', 'UNCLEAR_POLICY_COUNT', 'POLICY_VIOLATION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['policy_violation'],
                    'order': 6
                },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 7
                },
                # 'agent_intervention': {
                #     'function': _get_metric_func('calculate_agent_intervention_metrics'),
                #     'columns': ['AGENT_INTERVENTION_ANALYSIS_SUMMARY', 'AGENT_INTERVENTION_SUMMARY_SUCCESS'],
                #     'depends_on_prompts': ['agent_intervention'],
                #     'order': 8
                # },
                'wrong_answer': {
                    'function': _get_metric_func('calculate_wrong_answer_percentage'),
                    'columns': ['WRONG_ANSWER_PERCENTAGE', 'WRONG_ANSWER_COUNT', 'WRONG_ANSWER_DENOMINATOR', 'WRONG_ANSWER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['wrong_answer'],
                    'order': 9
                },
                'unsatisfactory_policy': {
                    'function': _get_metric_func('calculate_unsatisfactory_policy_percentage'),
                    'columns': ['UNSATISFACTORY_POLICY_PERCENTAGE', 'UNSATISFACTORY_POLICY_COUNT', 'UNSATISFACTORY_POLICY_DENOMINATOR', 'UNSATISFACTORY_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['unsatisfactory_policy'],
                    'order': 10
                },
                'dissatisfaction': {
                    'function': _get_metric_func('calculate_dissatisfaction_percentage'),
                    'columns': ['DISSATISFACTION_PERCENTAGE', 'DISSATISFACTION_COUNT', 'DISSATISFACTION_DENOMINATOR', 'DISSATISFACTION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['dissatisfaction'],
                    'order': 11
                },
                'de_escalation': {
                    'function': _get_metric_func('calculate_de_escalation_success_percentage'),
                    'columns': ['DE_ESCALATION_SUCCESS_PERCENTAGE', 'DE_ESCALATION_RESOLVED_COUNT', 'DE_ESCALATION_TOTAL_COUNT', 'DE_ESCALATION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['de_escalation'],
                    'order': 12
                },
                # 'threatening': {
                #     'function': _get_metric_func('calculate_threatening_percentage'),
                #     'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['threatening'],
                #     'order': 13
                # },
            }
        },
        'MV_Sales': {
            'master_table': 'MV_SALES_SUMMARY',
            'metrics': {
                # 'weighted_nps': {
                #     'function': _get_metric_func('calculate_weighted_nps_per_department'),
                #     'columns': ['WEIGHTED_AVG_NPS', 'WEIGHTED_AVG_NPS_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['SA_prompt'],
                #     'order': 1
                # },
                # 'client_suspecting_ai': {
                #     'function': _get_metric_func('calculate_client_suspecting_ai_percentage'),
                #     'columns': ['CLIENT_SUSPECTING_AI_COUNT', 'CLIENT_SUSPECTING_AI_PERCENTAGE', 'CLIENT_SUSPECTING_AI_DENOMINATOR', 'CLIENT_SUSPECTING_AI_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['client_suspecting_ai'],
                #     'order': 2
                # },
                'clarity_score': {
                    'function': _get_metric_func('calculate_clarity_score_percentage'),
                    'columns': ['CLARITY_SCORE_PERCENTAGE', 'CLARITY_SCORE_COUNT', 'CLARITY_SCORE_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['clarity_score'],
                    'order': 3
                },
                'total_chats': {
                    'function': _get_metric_func('calculate_total_chats'),
                    'columns': ['TOTAL_CHATS'],
                    'depends_on_prompts': ['SA_prompt'],
                    'order': 4
                },
                # 'sales_transfer': {
                #     'function': _get_metric_func('calculate_sales_transer_percentage'),
                #     'columns': ['TRANSFER_ESCALATION_PERCENTAGE_A', 'TRANSFER_ESCALATION_PERCENTAGE_B', 'TRANSFER_ESCALATION_COUNT', 'TRANSFER_KNOWN_FLOW_PERCENTAGE_A', 'TRANSFER_KNOWN_FLOW_PERCENTAGE_B', 'TRANSFER_KNOWN_FLOW_COUNT', 'TRANSFER_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['sales_transfer'],
                #     'order': 5
                # },
                # 'tool': {
                #     'function': _get_metric_func('generate_tool_summary_report'),
                #     'columns': ['WRONG_TOOL_PERCENTAGE', 'WRONG_TOOL_COUNT', 'MISSING_TOOL_PERCENTAGE', 'MISSING_TOOL_COUNT', 'TOOL_ANALYSIS_SUMMARY', 'TOOL_SUMMARY_SUCCESS'],
                #     'depends_on_prompts': ['tool'],
                #     'order': 6
                # },
                'policy_violation': {
                    'function': _get_metric_func('calculate_cc_sales_policy_violation_metrics'),
                    'columns': ['MISSING_POLICY_PERCENTAGE', 'MISSING_POLICY_COUNT', 'UNCLEAR_POLICY_PERCENTAGE', 'UNCLEAR_POLICY_COUNT', 'POLICY_VIOLATION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['policy_violation'],
                    'order': 7
                },
                'wrong_answer': {
                    'function': _get_metric_func('calculate_wrong_answer_percentage'),
                    'columns': ['WRONG_ANSWER_PERCENTAGE', 'WRONG_ANSWER_COUNT', 'WRONG_ANSWER_DENOMINATOR', 'WRONG_ANSWER_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['wrong_answer'],
                    'order': 8
                },
                'unsatisfactory_policy': {
                    'function': _get_metric_func('calculate_unsatisfactory_policy_percentage'),
                    'columns': ['UNSATISFACTORY_POLICY_PERCENTAGE', 'UNSATISFACTORY_POLICY_COUNT', 'UNSATISFACTORY_POLICY_DENOMINATOR', 'UNSATISFACTORY_POLICY_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['unsatisfactory_policy'],
                    'order': 9
                },
                'dissatisfaction': {
                    'function': _get_metric_func('calculate_dissatisfaction_percentage'),
                    'columns': ['DISSATISFACTION_PERCENTAGE', 'DISSATISFACTION_COUNT', 'DISSATISFACTION_DENOMINATOR', 'DISSATISFACTION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['dissatisfaction'],
                    'order': 10
                },
                'de_escalation': {
                    'function': _get_metric_func('calculate_de_escalation_success_percentage'),
                    'columns': ['DE_ESCALATION_SUCCESS_PERCENTAGE', 'DE_ESCALATION_RESOLVED_COUNT', 'DE_ESCALATION_TOTAL_COUNT', 'DE_ESCALATION_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['de_escalation'],
                    'order': 11
                },
                # 'threatening': {
                #     'function': _get_metric_func('calculate_threatening_percentage'),
                #     'columns': ['THREATENING_PERCENTAGE', 'THREATENING_COUNT', 'THREATENING_DENOMINATOR', 'THREATENING_SUMMARY_SUCCESS', 'THREATENING_ANALYSIS_SUMMARY'],
                #     'depends_on_prompts': ['threatening'],
                #     'order': 12
                # },
                'tool_eval': {
                    'function': _get_metric_func('calculate_tool_eval_metrics'),
                    'columns': ['TOOL_EVAL_WRONG_PCT', 'TOOL_EVAL_MISSING_PCT', 'TOOL_EVAL_TOTAL_MESSAGES', 'TOOL_EVAL_SUMMARY_SUCCESS', 'TOOL_EVAL_ANALYSIS_SUMMARY'],
                    'depends_on_prompts': ['tool_eval'],
                    'order': 13
                },
            }
        },
        # Task 39 — Prospect Nationality Service (Sales Nationality Identification Flow)
        'Prospect_Nationality_Service': {
            'master_table': 'PROSPECT_NATIONALITY_SERVICE_SUMMARY',
            'metrics': {
                # Metric 1 & 3: Transfer Correctness (routing accuracy) + Wrong Transfers
                'transfer_correctness': {
                    'function': _get_metric_func('calculate_prospect_transfer_correctness_metrics'),
                    'columns': [
                        'CORRECT_TRANSFER_CC_PERCENTAGE', 'CORRECT_TRANSFER_CC_COUNT',
                        'CORRECT_TRANSFER_MV_PERCENTAGE', 'CORRECT_TRANSFER_MV_COUNT',
                        'CORRECT_TRANSFER_UNKNOWN_PERCENTAGE', 'CORRECT_TRANSFER_UNKNOWN_COUNT',
                        'WRONG_TRANSFER_PERCENTAGE', 'WRONG_TRANSFER_COUNT',
                        'TRANSFER_DISTRIBUTION_CC_PERCENTAGE', 'TRANSFER_DISTRIBUTION_MV_PERCENTAGE',
                        'TRANSFER_DISTRIBUTION_UNKNOWN_PERCENTAGE', 'TRANSFER_DISTRIBUTION_NO_ROUTING_PERCENTAGE',
                        'TOTAL_TRANSFERS', 'TRANSFER_CORRECTNESS_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['routing_identification'],
                    'order': 1
                },
                # Metric 4: No Reply Rate
                'no_reply': {
                    'function': _get_metric_func('calculate_prospect_no_reply_metrics'),
                    'columns': [
                        'NO_REPLY_PERCENTAGE', 'NO_REPLY_COUNT', 'NO_REPLY_DENOMINATOR',
                        'NO_REPLY_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['engagement_outcomes'],
                    'order': 2
                },
                # Metric 5: Drop-off per question (Q1 and Q2)
                'dropoff': {
                    'function': _get_metric_func('calculate_prospect_dropoff_metrics'),
                    'columns': [
                        'DROPOFF_Q1_PERCENTAGE', 'DROPOFF_Q1_COUNT', 'DROPOFF_Q1_DENOMINATOR',
                        'DROPOFF_Q2_PERCENTAGE', 'DROPOFF_Q2_COUNT', 'DROPOFF_Q2_DENOMINATOR',
                        'DROPOFF_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['engagement_outcomes'],
                    'order': 3
                },
                # Metric 6: Agent Involvement (non-LLM, calculates from source columns)
                'agent_involvement': {
                    'function': _get_metric_func('calculate_prospect_agent_involvement_metrics'),
                    'columns': [
                        'AGENT_INVOLVEMENT_PERCENTAGE', 'AGENT_INVOLVEMENT_COUNT',
                        'AGENT_INVOLVEMENT_DENOMINATOR', 'AGENT_INVOLVEMENT_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': [],
                    'order': 4
                },
                # Metric 7: Required Tools Not Called
                'required_tools_not_called': {
                    'function': _get_metric_func('calculate_prospect_required_tools_not_called_metrics'),
                    'columns': [
                        'REQUIRED_TOOLS_NOT_CALLED_PERCENTAGE', 'REQUIRED_TOOLS_NOT_CALLED_COUNT',
                        'REQUIRED_TOOLS_NOT_CALLED_DENOMINATOR', 'REQUIRED_TOOLS_NOT_CALLED_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['routing_identification'],
                    'order': 5
                },
                # Metric 11: Chats Shadowed (non-LLM, calculates from IS_SHADOWED column)
                'chats_shadowed': {
                    'function': _get_metric_func('calculate_prospect_chats_shadowed_metrics'),
                    'columns': [
                        'CHATS_SHADOWED_PERCENTAGE', 'CHATS_SHADOWED_COUNT',
                        'CHATS_SHADOWED_DENOMINATOR', 'CHATS_SHADOWED_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': [],
                    'order': 6
                },
                # Metric 8: Cost (placeholder - COST column not in source table)
                'cost': {
                    'function': _get_metric_func('calculate_prospect_cost_metrics'),
                    'columns': [
                        'TOTAL_COST', 'AVG_COST_PER_CHAT', 'COST_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': [],
                    'order': 7
                },
                # Metric 9: Total Transfers (daily breakdown by destination)
                'total_transfers': {
                    'function': _get_metric_func('calculate_prospect_total_transfers_metrics'),
                    'columns': [
                        'DAILY_TRANSFERS_MV', 'DAILY_TRANSFERS_CC', 'DAILY_TRANSFERS_UNKNOWN',
                        'DAILY_TRANSFERS_NO_ROUTING', 'TOTAL_TRANSFERS_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['routing_identification'],
                    'order': 8
                },
                # Metric 10: In-chat poke / re-engagement rate
                'poke_reengagement': {
                    'function': _get_metric_func('calculate_prospect_poke_reengagement_metrics'),
                    'columns': [
                        'POKE_REENGAGEMENT_PERCENTAGE', 'POKED_CHATS_COUNT',
                        'REENGAGED_AFTER_POKE_COUNT', 'POKE_REENGAGEMENT_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': [],
                    'order': 9
                },
                # Metric 12: Chats fully handled by bot (no agent involvement + completed flow)
                'fully_handled_by_bot': {
                    'function': _get_metric_func('calculate_prospect_fully_handled_by_bot_metrics'),
                    'columns': [
                        'FULLY_HANDLED_BY_BOT_PERCENTAGE', 'FULLY_HANDLED_BY_BOT_COUNT',
                        'FULLY_HANDLED_BY_BOT_DENOMINATOR', 'FULLY_HANDLED_BY_BOT_ANALYSIS_SUMMARY'
                    ],
                    'depends_on_prompts': ['routing_identification'],
                    'order': 10
                },
                # Metric 13: LLM model configuration (metadata)
                'llm_model_config': {
                    'function': _get_metric_func('get_prospect_llm_model_config_metrics'),
                    'columns': [
                        'PRIMARY_LLM_MODEL', 'BACKUP_LLM_MODEL', 'LLM_MODEL_CONFIG_SUMMARY'
                    ],
                    'depends_on_prompts': [],
                    'order': 11
                }
            }
        }
    }


def get_department_summary_schema(department_name):
    """
    Generate dynamic table schema based on department's metrics configuration
    
    Args:
        department_name: Department name to get schema for
    
    Returns:
        Dictionary of column_name: data_type for CREATE TABLE
    """
    base_columns = {
        'DATE': 'DATE',
        'DEPARTMENT': 'VARCHAR(100)',
        'TIMESTAMP': 'TIMESTAMP'
    }
    
    metrics_config = get_metrics_configuration()
    dept_config = metrics_config.get(department_name, {})
    dept_metrics = dept_config.get('metrics', {})
    
    # Add metric columns (numeric or text based on column name)
    metric_columns = {}
    for metric_name, metric_config in dept_metrics.items():
        for col in metric_config['columns']:
            if col.upper() == 'DOCUMENT_REQUEST_ANALYSIS_SUMMARY':
                metric_columns[col] = 'TEXT'
            else:
                metric_columns[col] = 'FLOAT'
    
    return {**base_columns, **metric_columns}


def list_all_master_tables():
    """
    Get list of all department master summary tables
    """
    tables = []
    metrics_config = get_metrics_configuration()
    
    for dept_name, dept_config in metrics_config.items():
        tables.append(dept_config['master_table'])
    
    return sorted(list(set(tables)))  # Remove duplicates and sort
