"""
LLM Integration Module for Snowflake Chat Analysis
Simple integration functions to add to the main snowflake file
"""

import snowflake.snowpark as snowpark
from datetime import datetime, timedelta

# Import all the LLM modules
from snowflake_llm_orchestrator import (
    main_llm_analysis,
    main_llm_test,
    main_llm_validate,
    analyze_llm_single_department
)
from snowflake_llm_config import (
    list_all_departments,
    get_department_prompt_types,
    list_all_output_tables
)


def llm_analysis_full_pipeline(session: snowpark.Session, target_date=None, execution_mode='synchronous'):
    """
    Run the complete LLM analysis pipeline for all departments - supports four execution modes
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis (defaults to yesterday)
        execution_mode: 'synchronous' | 'batch_submit' | 'batch_fetch' | 'batch_cancel' (default: 'synchronous')
    
    Returns:
        Analysis results
    """
    mode_label = {
        'synchronous': 'SYNCHRONOUS',
        'batch_submit': 'BATCH SUBMIT (PHASE 1)',
        'batch_fetch': 'BATCH FETCH (PHASE 2)',
        'batch_cancel': 'BATCH CANCEL'
    }
    
    print(f"🚀 STARTING COMPLETE LLM ANALYSIS PIPELINE - {mode_label.get(execution_mode, execution_mode)}")
    print("=" * 60)
    
    # Set default target date
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    try:
        # Step 1: Validate dependencies (skip for batch_fetch mode)
        if execution_mode != 'batch_fetch':
            print("📋 Step 1: Validating dependencies...")
            validation_results = main_llm_validate(session)
            
            if validation_results['overall_status'] != 'SUCCESS':
                print("❌ Dependency validation failed - aborting pipeline")
                return {
                    'success': False,
                    'error': 'Dependency validation failed',
                    'validation_results': validation_results
                }
            
            print("✅ Dependencies validated successfully")
        else:
            validation_results = {'overall_status': 'SKIPPED', 'note': 'Validation skipped for batch_fetch mode'}
        
        # Step 2: Run complete analysis with execution mode
        print(f"\n🤖 Step 2: Running LLM analysis for {target_date} ({execution_mode} mode)...")
        analysis_results = main_llm_analysis(session, target_date, execution_mode=execution_mode)
        
        # Step 3: Generate final summary
        success = 'error' not in analysis_results
        
        pipeline_summary = f"""
🎯 LLM ANALYSIS PIPELINE - COMPLETE
{'=' * 50}
📅 Date: {target_date}
✅ Status: {'SUCCESS' if success else 'FAILED'}
📊 Results: {analysis_results.get('statistics', {}).get('total_processed', 0)} conversations processed
💾 Tables: {len(list_all_output_tables())} output tables created

{'🌟 Pipeline completed successfully!' if success else '❌ Pipeline failed - check logs for details'}
"""
        
        print(pipeline_summary)
        
        return {
            'success': success,
            'target_date': target_date,
            'pipeline_summary': pipeline_summary,
            'validation_results': validation_results,
            'analysis_results': analysis_results
        }
        
    except Exception as e:
        error_msg = f"Pipeline failed: {str(e)}"
        print(f"❌ {error_msg}")
        return {
            'success': False,
            'error': error_msg,
            'target_date': target_date
        }


def llm_analysis_quick_test(session: snowpark.Session, department_name='CC_Resolvers', target_date=None, prompts=['*'], metrics=['*'], execution_mode='synchronous', max_conversations: int = None):
    """
    Run a quick test of LLM analysis on a single department - supports four execution modes
    
    Args:
        session: Snowflake session
        department_name: Department to test (defaults to CC_Resolvers)
        target_date: Target date for testing
        prompts: List of prompt types to test (defaults to all prompts)
        metrics: List of metrics to calculate (defaults to all metrics)
        execution_mode: 'synchronous' | 'batch_submit' | 'batch_fetch' | 'batch_cancel' (default: 'synchronous')
        max_conversations: Optional int. If set in synchronous mode, limit prompt processing to at most this many
                          unique CONVERSATION_IDs (useful for testing cost control).
    
    Returns:
        Test results
    """
    mode_label = {
        'synchronous': 'SYNCHRONOUS',
        'batch_submit': 'BATCH SUBMIT (PHASE 1)',
        'batch_fetch': 'BATCH FETCH (PHASE 2)',
        'batch_cancel': 'BATCH CANCEL'
    }
    
    print(f"🧪 QUICK LLM TEST - {department_name} - {mode_label.get(execution_mode, execution_mode)}")
    print("=" * 50)
    
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    try:
        # Validate dependencies first (skip for batch_fetch mode)
        if execution_mode != 'batch_fetch':
            validation_results = main_llm_validate(session)
            if validation_results['overall_status'] != 'SUCCESS':
                return {
                    'success': False,
                    'error': 'Dependency validation failed',
                    'validation_results': validation_results
                }
        else:
            validation_results = {'overall_status': 'SKIPPED', 'note': 'Validation skipped for batch_fetch mode'}
        
        # Run single department analysis with execution mode
        results = analyze_llm_single_department(
            session,
            department_name,
            target_date,
            prompts,
            metrics,
            execution_mode=execution_mode,
            max_conversations=max_conversations,
        )
        
        test_summary = f"""
🧪 QUICK TEST RESULTS
{'=' * 30}
Department: {department_name}
Date: {target_date}
Status: {'SUCCESS' if results['success'] else 'FAILED'}
{'Error: ' + results.get('error', '') if not results['success'] else ''}

{'✅ Quick test passed!' if results['success'] else '❌ Quick test failed!'}
"""
        
        print(test_summary)
        
        return {
            'success': results['success'],
            'department': department_name,
            'target_date': target_date,
            'test_summary': test_summary,
            'results': results
        }
        
    except Exception as e:
        error_msg = f"Quick test failed: {str(e)}"
        print(f"❌ {error_msg}")
        return {
            'success': False,
            'error': error_msg,
            'department': department_name,
            'target_date': target_date
        }


def llm_analysis_status_check(session: snowpark.Session):
    """
    Check the status of LLM analysis system and configuration
    
    Args:
        session: Snowflake session
    
    Returns:
        Status information
    """
    print("📊 LLM ANALYSIS STATUS CHECK")
    print("=" * 40)
    
    try:
        # Get configuration info
        departments = list_all_departments()
        output_tables = list_all_output_tables()
        
        # Count prompts per department
        prompt_counts = {}
        total_prompts = 0
        
        for dept in departments:
            prompts = get_department_prompt_types(dept)
            prompt_counts[dept] = len(prompts)
            total_prompts += len(prompts)
        
        # Check table existence
        existing_tables = 0
        for table in output_tables:
            try:
                session.sql(f"SELECT COUNT(*) FROM LLM_EVAL.PUBLIC.{table}").collect()
                existing_tables += 1
            except:
                pass
        
        status_info = {
            'departments_configured': len(departments),
            'total_prompts': total_prompts,
            'output_tables_defined': len(output_tables),
            'output_tables_existing': existing_tables,
            'prompt_counts_by_department': prompt_counts
        }
        
        status_summary = f"""
📊 LLM ANALYSIS STATUS
{'=' * 30}
🏢 Departments: {len(departments)} configured
🎯 Prompts: {total_prompts} total prompts
💾 Tables: {existing_tables}/{len(output_tables)} tables exist
📋 Configuration: {'✅ Valid' if len(departments) > 0 else '❌ Invalid'}

Department breakdown:
{chr(10).join([f'  • {dept}: {count} prompts' for dept, count in prompt_counts.items()])}
"""
        
        print(status_summary)
        
        return {
            'status': 'healthy' if len(departments) > 0 and total_prompts > 0 else 'unhealthy',
            'status_summary': status_summary,
            'details': status_info
        }
        
    except Exception as e:
        error_msg = f"Status check failed: {str(e)}"
        print(f"❌ {error_msg}")
        return {
            'status': 'error',
            'error': error_msg
        }


def llm_analysis_cleanup_tables(session: snowpark.Session, confirm=False):
    """
    Clean up all LLM analysis tables (use with caution!)
    
    Args:
        session: Snowflake session
        confirm: Must be True to actually perform cleanup
    
    Returns:
        Cleanup results
    """
    if not confirm:
        print("⚠️  DRY RUN: Cleanup not confirmed. Set confirm=True to actually delete tables.")
        tables = list_all_output_tables()
        print(f"Would delete {len(tables)} tables:")
        for table in tables:
            print(f"  • LLM_EVAL.PUBLIC.{table}")
        return {'dry_run': True, 'tables_to_delete': len(tables)}
    
    print("🧹 CLEANING UP LLM ANALYSIS TABLES")
    print("⚠️  WARNING: This will delete all LLM analysis data!")
    print("=" * 50)
    
    try:
        tables = list_all_output_tables()
        tables.append('LLM_EVALS_SUMMARY')  # Add master summary table
        
        deleted_count = 0
        failed_count = 0
        
        for table in tables:
            try:
                session.sql(f"DROP TABLE IF EXISTS LLM_EVAL.PUBLIC.{table}").collect()
                print(f"  ✅ Deleted: {table}")
                deleted_count += 1
            except Exception as e:
                print(f"  ❌ Failed to delete {table}: {str(e)}")
                failed_count += 1
        
        cleanup_summary = f"""
🧹 CLEANUP COMPLETE
{'=' * 20}
✅ Deleted: {deleted_count} tables
❌ Failed: {failed_count} tables
Status: {'SUCCESS' if failed_count == 0 else 'PARTIAL'}
"""
        
        print(cleanup_summary)
        
        return {
            'success': failed_count == 0,
            'deleted_count': deleted_count,
            'failed_count': failed_count,
            'cleanup_summary': cleanup_summary
        }
        
    except Exception as e:
        error_msg = f"Cleanup failed: {str(e)}"
        print(f"❌ {error_msg}")
        return {
            'success': False,
            'error': error_msg
        }


# Easy-to-use wrapper functions for main file integration
def run_llm_analysis(session: snowpark.Session, target_date=None, execution_mode='synchronous'):
    """
    Simple wrapper for running complete LLM analysis - supports four execution modes
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis (defaults to yesterday)
        execution_mode: 'synchronous' | 'batch_submit' | 'batch_fetch' | 'batch_cancel' (default: 'synchronous')
    """
    return llm_analysis_full_pipeline(session, target_date, execution_mode=execution_mode)


def test_llm_analysis(session: snowpark.Session, department='CC_Resolvers', target_date=None, prompts=['*'], metrics=['*'], execution_mode='synchronous', max_conversations: int = None):
    """
    Simple wrapper for testing LLM analysis with dynamic prompt/metric selection - supports four execution modes
    
    Args:
        session: Snowflake session
        department: Department to test (defaults to CC_Resolvers)
        target_date: Target date for analysis (defaults to yesterday)
        prompts: List of prompt types to test (defaults to all prompts)
        metrics: List of metrics to calculate (defaults to all metrics)
        execution_mode: 'synchronous' | 'batch_submit' | 'batch_fetch' | 'batch_cancel' (default: 'synchronous')
        max_conversations: Optional int. If set in synchronous mode, limit prompt processing to at most this many
                          unique CONVERSATION_IDs (useful for testing cost control).
    """
    return llm_analysis_quick_test(session, department, target_date, prompts, metrics, execution_mode=execution_mode, max_conversations=max_conversations)


def check_llm_status(session: snowpark.Session):
    """Simple wrapper for checking LLM status"""
    return llm_analysis_status_check(session)


def validate_llm_setup(session: snowpark.Session):
    """Simple wrapper for validating LLM setup"""
    return main_llm_validate(session)


# For adding to main snowflake file - examples of how to use
INTEGRATION_EXAMPLES = """
# Add these lines to your main snowflake file to use LLM analysis:

# Import the integration module
from snowflake_llm_integration import (
    run_llm_analysis,
    test_llm_analysis, 
    check_llm_status,
    validate_llm_setup
)

# Example usage in main function:
def main(session):
    # Option 1: Run complete analysis
    result = run_llm_analysis(session, '2025-07-22')
    return result['pipeline_summary']
    
    # Option 2: Test with single department
    result = test_llm_analysis(session, 'CC_Resolvers', '2025-07-22')
    return result['test_summary']
    
    # Option 3: Check system status
    result = check_llm_status(session)
    return result['status_summary']
    
    # Option 4: Validate setup
    result = validate_llm_setup(session)
    return f"LLM Setup: {result['overall_status']}"
"""