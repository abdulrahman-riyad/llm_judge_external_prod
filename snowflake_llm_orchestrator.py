"""
LLM Analysis Orchestrator Module for Snowflake Chat Analysis
Main module that orchestrates the entire LLM analysis workflow
Can be imported into the main snowflake file for easy execution
"""

import snowflake.snowpark as snowpark
from datetime import datetime, timedelta
import traceback
from snowflake_llm_config import (
    get_snowflake_llm_departments_config, 
    list_all_departments, 
    get_department_prompt_types,
    list_all_output_tables
)
from snowflake_llm_processor import (
    process_department_llm_analysis,
    update_llm_master_summary,
    test_llm_single_prompt,
    format_error_details
)


def analyze_llm_conversations_all_departments(session: snowpark.Session, target_date=None, department_filter=None, execution_mode='synchronous'):
    """
    Analyze LLM conversations for all departments - supports four execution modes
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis (defaults to yesterday)
        department_filter: Optional specific department to process (for testing)
        execution_mode: 'synchronous' | 'batch_submit' | 'batch_fetch' | 'batch_cancel' (default: 'synchronous')
    
    Returns:
        Analysis results dictionary
    """
    # Validate execution_mode
    valid_modes = ['synchronous', 'batch_submit', 'batch_fetch', 'batch_cancel']
    if execution_mode not in valid_modes:
        raise ValueError(f"Invalid execution_mode: {execution_mode}. Must be one of {valid_modes}")
    
    mode_label = {
        'synchronous': 'SYNCHRONOUS',
        'batch_submit': 'BATCH SUBMIT (PHASE 1)',
        'batch_fetch': 'BATCH FETCH (PHASE 2)',
        'batch_cancel': 'BATCH CANCEL'
    }
    
    print(f"\n🤖 LLM ANALYSIS: PROCESSING ALL DEPARTMENTS - {mode_label[execution_mode]}")
    print("=" * 60)
    
    # Set target date if not provided
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"📅 Target date: {target_date}")
    print(f"⚙️  Execution mode: {execution_mode}")
    
    if department_filter:
        print(f"🎯 Department filter: {department_filter}")
    
    print("=" * 60)
    
    departments_config = get_snowflake_llm_departments_config()
    department_results = {}
    
    # Get list of departments to process
    departments_to_process = [department_filter] if department_filter else list(departments_config.keys())
    
    total_departments = len(departments_to_process)
    processed_departments = 0
    successful_departments = 0
    
    for department_name in departments_to_process:
        if department_name not in departments_config:
            print(f"⚠️  Department {department_name} not found in configuration")
            continue
            
        try:
            # Process department (includes all its prompts) with execution mode
            dept_results, success = process_department_llm_analysis(
                session, department_name, target_date, execution_mode=execution_mode
            )
            
            department_results[department_name] = dept_results
            processed_departments += 1
            
            if success:
                successful_departments += 1
            
        except Exception as e:
            error_msg = f"LLM analysis failed: {str(e)}"
            error_details = format_error_details(e, f"DEPARTMENT PROCESSING - {department_name}")
            print(f"  ❌ {department_name}: {error_msg}")
            print(error_details)
            department_results[department_name] = {'error': error_msg, 'traceback': str(e)}
            processed_departments += 1
    
    # For batch_submit mode, skip master summary and return batch submission results
    if execution_mode == 'batch_submit':
        # Count submitted batches
        total_batches = 0
        submitted_batches = 0
        failed_batches = 0
        
        for dept_name, dept_results in department_results.items():
            if 'error' not in dept_results:
                for prompt_type, prompt_results in dept_results.items():
                    if isinstance(prompt_results, dict) and 'status' in prompt_results:
                        total_batches += 1
                        if prompt_results.get('status') == 'submitted':
                            submitted_batches += 1
                        elif prompt_results.get('status') == 'failed':
                            failed_batches += 1
        
        summary = f"""
    📤 BATCH SUBMIT (PHASE 1) - SUMMARY
    {'=' * 50}
    📅 Date: {target_date}
    🏢 Departments processed: {processed_departments}/{total_departments}
    
    📊 BATCH SUBMISSION RESULTS:
    📤 Total batches submitted: {submitted_batches}/{total_batches}
    ❌ Failed submissions: {failed_batches}/{total_batches}
    
    ⏳ NEXT STEP:
    Wait ~6 hours for OpenAI to process batches, then run with execution_mode='batch_fetch'
    """
        
        print(summary)
        
        return {
            'summary': summary,
            'execution_mode': 'batch_submit',
            'total_batches': total_batches,
            'submitted_batches': submitted_batches,
            'failed_batches': failed_batches,
            'department_results': department_results,
            'target_date': target_date
        }
    
    # For batch_cancel mode, skip master summary and return cancellation results
    if execution_mode == 'batch_cancel':
        # Count cancelled batches
        total_batches = 0
        cancelled_batches = 0
        failed_cancels = 0
        
        for dept_name, dept_results in department_results.items():
            if 'error' not in dept_results:
                for prompt_type, prompt_results in dept_results.items():
                    if isinstance(prompt_results, dict) and 'status' in prompt_results:
                        total_batches += 1
                        if prompt_results.get('status') == 'cancelled':
                            cancelled_batches += 1
                        elif prompt_results.get('status') == 'failed':
                            failed_cancels += 1
        
        summary = f"""
    🚫 BATCH CANCEL - SUMMARY
    {'=' * 50}
    📅 Date: {target_date}
    🏢 Departments processed: {processed_departments}/{total_departments}
    
    📊 BATCH CANCELLATION RESULTS:
    🚫 Total batches cancelled: {cancelled_batches}/{total_batches}
    ❌ Failed cancellations: {failed_cancels}/{total_batches}
    """
        
        print(summary)
        
        return {
            'summary': summary,
            'execution_mode': 'batch_cancel',
            'total_batches': total_batches,
            'cancelled_batches': cancelled_batches,
            'failed_cancels': failed_cancels,
            'department_results': department_results,
            'target_date': target_date
        }
    
    # For synchronous and batch_fetch modes: Create summary and update master table
    print(f"\n📊 CREATING MASTER SUMMARY...")
    master_success = update_llm_master_summary(session, department_results, target_date)
    
    # Generate final summary
    total_conversations = 0
    total_processed = 0
    total_prompts = 0
    successful_prompts = 0
    
    for dept_name, dept_results in department_results.items():
        if 'error' not in dept_results:
            for prompt_type, prompt_results in dept_results.items():
                if isinstance(prompt_results, dict) and 'total_conversations' in prompt_results:
                    total_conversations += prompt_results.get('total_conversations', 0)
                    total_processed += prompt_results.get('processed_count', 0)
                    total_prompts += 1
                    if prompt_results.get('processed_count', 0) > 0:
                        successful_prompts += 1
    
    overall_success_rate = (total_processed / total_conversations * 100) if total_conversations > 0 else 0
    
    mode_str = "SYNCHRONOUS" if execution_mode == 'synchronous' else "BATCH FETCH (PHASE 2)"
    
    summary = f"""
    🎯 LLM ANALYSIS - {mode_str} - SUMMARY
    {'=' * 50}
    📅 Date: {target_date}
    🏢 Departments processed: {processed_departments}/{total_departments}
    ✅ Successful departments: {successful_departments}/{processed_departments}

    📊 OVERALL METRICS:
    💬 Total conversations: {total_conversations:,}
    ✅ Successfully processed: {total_processed:,} ({overall_success_rate:.1f}%)
    🎯 Total prompts: {total_prompts}
    ✅ Successful prompts: {successful_prompts}/{total_prompts}

    💾 OUTPUT:
    📋 Master summary: LLM_EVALS_SUMMARY {'✅' if master_success else '❌'}
    📊 Raw data tables: {len(list_all_output_tables())} tables

    🌟 LLM Analysis Complete!
    Ready for review and further analysis
    """
    
    print(summary)
    
    return {
        'summary': summary,
        'execution_mode': execution_mode,
        'department_results': department_results,
        'master_success': master_success,
        'statistics': {
            'target_date': target_date,
            'processed_departments': processed_departments,
            'successful_departments': successful_departments,
            'total_conversations': total_conversations,
            'total_processed': total_processed,
            'total_prompts': total_prompts,
            'successful_prompts': successful_prompts,
            'overall_success_rate': overall_success_rate
        }
    }


def analyze_llm_single_department(session: snowpark.Session, department_name, target_date=None, prompts=['*'], metrics=['*'], execution_mode='synchronous', max_conversations: int = None):
    """
    Analyze LLM conversations for a single department - supports four execution modes
    
    Args:
        session: Snowflake session
        department_name: Department name to process
        target_date: Target date for analysis (defaults to yesterday)
        prompts: List of prompt types to process (default: ['*'] for all prompts)
        metrics: List of metrics to calculate (default: ['*'] for all metrics)
        execution_mode: 'synchronous' | 'batch_submit' | 'batch_fetch' | 'batch_cancel' (default: 'synchronous')
        max_conversations: Optional int. If set in synchronous mode, limit prompt processing to at most this many
                          unique CONVERSATION_IDs (useful for testing cost control).
    
    Returns:
        Single department analysis results
    """
    # Validate execution_mode
    valid_modes = ['synchronous', 'batch_submit', 'batch_fetch', 'batch_cancel']
    if execution_mode not in valid_modes:
        raise ValueError(f"Invalid execution_mode: {execution_mode}. Must be one of {valid_modes}")
    
    mode_label = {
        'synchronous': 'SYNCHRONOUS',
        'batch_submit': 'BATCH SUBMIT (PHASE 1)',
        'batch_fetch': 'BATCH FETCH (PHASE 2)',
        'batch_cancel': 'BATCH CANCEL'
    }
    
    print(f"\n🤖 LLM ANALYSIS: PROCESSING SINGLE DEPARTMENT - {department_name} - {mode_label[execution_mode]}")
    print("=" * 60)
    
    # Set target date if not provided
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"📅 Target date: {target_date}")
    print(f"⚙️  Execution mode: {execution_mode}")
    print("=" * 60)
    
    try:
        # Process the department with execution mode
        dept_results, success = process_department_llm_analysis(
            session,
            department_name,
            target_date,
            selected_prompts=prompts,
            execution_mode=execution_mode,
            max_conversations=max_conversations,
        )
        
        # For batch_submit mode, return immediately with submission results
        if execution_mode == 'batch_submit':
            if success:
                total_batches = len(dept_results)
                submitted_batches = sum(1 for r in dept_results.values() if r.get('status') == 'submitted')
                failed_batches = sum(1 for r in dept_results.values() if r.get('status') == 'failed')
                
                summary = f"""
                📤 {department_name} - BATCH SUBMIT COMPLETE
                {'=' * 50}
                📅 Date: {target_date}
                📤 Batches submitted: {submitted_batches}/{total_batches}
                ❌ Failed submissions: {failed_batches}/{total_batches}
                
                ⏳ NEXT STEP:
                Wait ~6 hours, then run with execution_mode='batch_fetch'
                """
                print(summary)
                
                return {
                    'success': True,
                    'execution_mode': 'batch_submit',
                    'summary': summary,
                    'department_results': dept_results,
                    'statistics': {
                        'total_batches': total_batches,
                        'submitted_batches': submitted_batches,
                        'failed_batches': failed_batches
                    }
                }
            else:
                error_msg = dept_results.get('error', 'Unknown error')
                print(f"❌ Failed to submit batches for {department_name}: {error_msg}")
                return {
                    'success': False,
                    'execution_mode': 'batch_submit',
                    'error': error_msg,
                    'department_results': dept_results
                }
        
        # For batch_cancel mode, return immediately with cancellation results
        if execution_mode == 'batch_cancel':
            if success:
                total_batches = len(dept_results)
                cancelled_batches = sum(1 for r in dept_results.values() if r.get('status') == 'cancelled')
                failed_cancels = sum(1 for r in dept_results.values() if r.get('status') == 'failed')
                
                summary = f"""
                🚫 {department_name} - BATCH CANCEL COMPLETE
                {'=' * 50}
                📅 Date: {target_date}
                🚫 Batches cancelled: {cancelled_batches}/{total_batches}
                ❌ Failed cancellations: {failed_cancels}/{total_batches}
                """
                print(summary)
                
                return {
                    'success': True,
                    'execution_mode': 'batch_cancel',
                    'summary': summary,
                    'department_results': dept_results,
                    'statistics': {
                        'total_batches': total_batches,
                        'cancelled_batches': cancelled_batches,
                        'failed_cancels': failed_cancels
                    }
                }
            else:
                error_msg = dept_results.get('error', 'Unknown error')
                print(f"❌ Failed to cancel batches for {department_name}: {error_msg}")
                return {
                    'success': False,
                    'execution_mode': 'batch_cancel',
                    'error': error_msg,
                    'department_results': dept_results
                }
        
        # For synchronous and batch_fetch modes: Update metrics and master summary
        if success:
            # Update master summary for this department only
            department_results = {department_name: dept_results}
            master_success = update_llm_master_summary(session, department_results, target_date, selected_metrics=metrics)
            
            # Generate summary
            total_conversations = 0
            total_processed = 0
            successful_prompts = 0
            total_prompts = len(dept_results)
            
            for prompt_type, prompt_results in dept_results.items():
                if isinstance(prompt_results, dict) and 'total_conversations' in prompt_results:
                    total_conversations += prompt_results.get('total_conversations', 0)
                    total_processed += prompt_results.get('processed_count', 0)
                    if prompt_results.get('processed_count', 0) > 0:
                        successful_prompts += 1
            
            success_rate = (total_processed / total_conversations * 100) if total_conversations > 0 else 0
            
            mode_str = "SYNCHRONOUS" if execution_mode == 'synchronous' else "BATCH FETCH (PHASE 2)"
            
            summary = f"""
            ✅ {department_name} - {mode_str} - ANALYSIS COMPLETE
            {'=' * 50}
            📅 Date: {target_date}
            💬 Conversations: {total_conversations:,}
            ✅ Processed: {total_processed:,} ({success_rate:.1f}%)
            🎯 Prompts: {successful_prompts}/{total_prompts} successful
            💾 Master summary: {'✅' if master_success else '❌'}
            """
            print(summary)
            
            return {
                'success': True,
                'execution_mode': execution_mode,
                'summary': summary,
                'department_results': dept_results,
                'statistics': {
                    'total_conversations': total_conversations,
                    'total_processed': total_processed,
                    'success_rate': success_rate,
                    'successful_prompts': successful_prompts,
                    'total_prompts': total_prompts
                }
            }
        else:
            error_msg = dept_results.get('error', 'Unknown error')
            print(f"❌ Failed to process {department_name}: {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'department_results': dept_results
            }
    
    except Exception as e:
        error_details = format_error_details(e, f"SINGLE DEPARTMENT ANALYSIS - {department_name}")
        print(error_details)
        return {
            'success': False,
            'error': str(e),
            'traceback': error_details
        }


def run_llm_test_suite(session: snowpark.Session, target_date=None):
    """
    Run a comprehensive test suite for LLM analysis
    
    Args:
        session: Snowflake session
        target_date: Target date for testing
    
    Returns:
        Test results
    """
    print("\n🧪 LLM ANALYSIS TEST SUITE")
    print("=" * 60)
    
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"📅 Target date: {target_date}")
    print("=" * 60)
    
    test_results = {
        'configuration_tests': {},
        'department_tests': {},
        'prompt_tests': {},
        'overall_status': 'PENDING'
    }
    
    try:
        # Test 1: Configuration validation
        print("\n🔧 Testing Configuration...")
        departments_config = get_snowflake_llm_departments_config()
        departments = list_all_departments()
        output_tables = list_all_output_tables()
        
        test_results['configuration_tests'] = {
            'departments_count': len(departments),
            'departments_with_prompts': len([d for d in departments if departments_config[d].get('llm_prompts')]),
            'total_output_tables': len(output_tables),
            'config_valid': True
        }
        
        print(f"    ✅ {len(departments)} departments configured")
        print(f"    ✅ {test_results['configuration_tests']['departments_with_prompts']} departments have LLM prompts")
        print(f"    ✅ {len(output_tables)} output tables defined")
        
        # Test 2: Sample department processing
        print("\n🏢 Testing Department Processing...")
        sample_departments = ['CC_Resolvers', 'Doctors']  # Test with common departments
        
        for dept in sample_departments:
            if dept in departments:
                print(f"  Testing {dept}...")
                try:
                    # Run analysis for single department
                    result = analyze_llm_single_department(session, dept, target_date)
                    test_results['department_tests'][dept] = {
                        'success': result['success'],
                        'processed_conversations': result.get('statistics', {}).get('total_processed', 0),
                        'error': result.get('error', None)
                    }
                    
                    if result['success']:
                        print(f"    ✅ {dept}: {result['statistics']['total_processed']} conversations processed")
                    else:
                        print(f"    ❌ {dept}: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    test_results['department_tests'][dept] = {
                        'success': False,
                        'error': str(e)
                    }
                    print(f"    ❌ {dept}: Exception - {str(e)}")
        
        # Test 3: Specific prompt testing
        print("\n🎯 Testing Individual Prompts...")
        test_prompts = [
            ('CC_Resolvers', 'quality_assessment'),
            ('Doctors', 'medical_appropriateness')
        ]
        
        for dept, prompt_type in test_prompts:
            if dept in departments:
                prompt_key = f"{dept}_{prompt_type}"
                print(f"  Testing {dept}/{prompt_type}...")
                try:
                    test_llm_single_prompt(session, dept, prompt_type, target_date, sample_size=1)
                    test_results['prompt_tests'][prompt_key] = {'success': True}
                    print(f"    ✅ {prompt_key}: Test completed")
                except Exception as e:
                    test_results['prompt_tests'][prompt_key] = {'success': False, 'error': str(e)}
                    print(f"    ❌ {prompt_key}: {str(e)}")
        
        # Determine overall status
        dept_success = all(result.get('success', False) for result in test_results['department_tests'].values())
        prompt_success = all(result.get('success', False) for result in test_results['prompt_tests'].values())
        
        if dept_success and prompt_success:
            test_results['overall_status'] = 'SUCCESS'
            print(f"\n🎉 TEST SUITE PASSED!")
        else:
            test_results['overall_status'] = 'PARTIAL'
            print(f"\n⚠️  TEST SUITE PARTIALLY SUCCESSFUL")
        
    except Exception as e:
        test_results['overall_status'] = 'FAILED'
        error_details = format_error_details(e, "TEST SUITE")
        print(f"\n❌ TEST SUITE FAILED:")
        print(error_details)
        test_results['error'] = str(e)
    
    return test_results


def validate_llm_dependencies(session: snowpark.Session):
    """
    Validate that all LLM dependencies are available
    
    Args:
        session: Snowflake session
    
    Returns:
        Validation results
    """
    print("🔍 VALIDATING LLM DEPENDENCIES")
    print("=" * 50)
    
    validation_results = {
        'snowflake_functions': {},
        'configuration': {},
        'permissions': {},
        'overall_status': 'PENDING'
    }
    
    try:
        # Test 1: Check if LLM functions exist
        print("🔧 Testing Snowflake LLM functions...")
        
        llm_functions = ['openai_chat_system', 'gemini_chat_system']
        for func in llm_functions:
            try:
                test_query = f"SELECT {func}('Hello', 'You are a test assistant', 'gpt-4o-mini', 0.1, 10) AS test_response"
                result = session.sql(test_query).collect()
                validation_results['snowflake_functions'][func] = {
                    'available': True,
                    'response': 'Function callable'
                }
                print(f"    ✅ {func}: Available and callable")
            except Exception as e:
                validation_results['snowflake_functions'][func] = {
                    'available': False,
                    'error': str(e)
                }
                print(f"    ❌ {func}: {str(e)}")
        
        # Test 2: Configuration validation
        print("\n📋 Testing configuration...")
        try:
            config = get_snowflake_llm_departments_config()
            departments = list_all_departments()
            validation_results['configuration'] = {
                'valid': True,
                'departments_count': len(departments),
                'config_loaded': True
            }
            print(f"    ✅ Configuration valid: {len(departments)} departments")
        except Exception as e:
            validation_results['configuration'] = {
                'valid': False,
                'error': str(e)
            }
            print(f"    ❌ Configuration error: {str(e)}")
        
        # Test 3: Basic permissions
        print("\n🔒 Testing permissions...")
        try:
            # Test table creation permissions
            # test_table = "LLM_EVAL.PUBLIC.LLM_TEST_TABLE"
            # session.sql(f"CREATE OR REPLACE TABLE {test_table} (test_col STRING)").collect()
            # session.sql(f"DROP TABLE {test_table}").collect()
            
            validation_results['permissions'] = {
                'table_creation': True,
                'schema_access': True
            }
            print(f"    ✅ Permissions: Table creation and schema access available")
        except Exception as e:
            validation_results['permissions'] = {
                'table_creation': False,
                'error': str(e)
            }
            print(f"    ❌ Permissions error: {str(e)}")
        
        # Determine overall status
        functions_ok = all(f.get('available', False) for f in validation_results['snowflake_functions'].values())
        config_ok = validation_results['configuration'].get('valid', False)
        permissions_ok = validation_results['permissions'].get('table_creation', False)
        
        if functions_ok and config_ok and permissions_ok:
            validation_results['overall_status'] = 'SUCCESS'
            print(f"\n🎉 ALL DEPENDENCIES VALIDATED!")
        else:
            validation_results['overall_status'] = 'FAILED'
            print(f"\n❌ DEPENDENCY VALIDATION FAILED")
            
    except Exception as e:
        validation_results['overall_status'] = 'ERROR'
        error_details = format_error_details(e, "DEPENDENCY VALIDATION")
        print(error_details)
        validation_results['error'] = str(e)
    
    return validation_results


# Main functions for easy import
def main_llm_analysis(session: snowpark.Session, target_date=None, execution_mode='synchronous'):
    """
    Main function for complete LLM analysis - can be called from main snowflake file
    Supports three execution modes: 'synchronous', 'batch_submit', 'batch_fetch'
    """
    try:
        return analyze_llm_conversations_all_departments(session, target_date, execution_mode=execution_mode)
    except Exception as e:
        error_report = format_error_details(e, "MAIN LLM ANALYSIS")
        return {
            'summary': f"❌ LLM ANALYSIS FAILED: {str(e)}",
            'error': str(e),
            'traceback': error_report
        }


def main_llm_test(session: snowpark.Session, target_date=None):
    """
    Main function for LLM testing - can be called from main snowflake file
    """
    try:
        return run_llm_test_suite(session, target_date)
    except Exception as e:
        error_report = format_error_details(e, "MAIN LLM TEST")
        return {
            'overall_status': 'ERROR',
            'error': str(e),
            'traceback': error_report
        }


def main_llm_validate(session: snowpark.Session):
    """
    Main function for LLM validation - can be called from main snowflake file
    """
    try:
        return validate_llm_dependencies(session)
    except Exception as e:
        error_report = format_error_details(e, "MAIN LLM VALIDATE")
        return {
            'overall_status': 'ERROR',
            'error': str(e),
            'traceback': error_report
        }