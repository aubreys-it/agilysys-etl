import logging, os
import azure.functions as func
import pymssql
import json
from datetime import datetime


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('payroll-log-vi-result: processing request.')

    # --- API key authentication ---
    api_key = req.headers.get('x-functions-key') or req.params.get('code')
    if api_key != os.environ['PAYROLL_DAEMON_API_KEY']:
        return func.HttpResponse('Unauthorized', status_code=401)

    # --- Parse request body ---
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse('Invalid JSON body', status_code=400)

    run_id       = body.get('run_id')
    payroll_date = body.get('payroll_date')
    status       = body.get('status')       # SUCCESS, FAILED, or WARNING
    message      = body.get('message')      # optional — error detail or None
    job_code     = body.get('job_code')     # e.g. VIWI01
    job_label    = body.get('job_label')    # e.g. Regular Payroll

    if not run_id or not payroll_date or not status:
        return func.HttpResponse(
            'Missing required fields: run_id, payroll_date, status',
            status_code=400
        )

    if status not in ('SUCCESS', 'FAILED', 'WARNING'):
        return func.HttpResponse(
            f'Invalid status: {status}. Must be SUCCESS, FAILED, or WARNING.',
            status_code=400
        )

    # Build a descriptive message if not provided
    if not message and job_code and job_label:
        if status == 'SUCCESS':
            message = f'{job_code} {job_label} completed successfully.'
        else:
            message = f'{job_code} {job_label} did not complete successfully.'

    # --- Connect to SQL ---
    try:
        conn = pymssql.connect(
            server   = os.environ['SQL_SERVER'],
            user     = os.environ['SQL_USER'],
            password = os.environ['SQL_PASSWORD'],
            database = os.environ['SQL_DATABASE']
        )
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f'SQL connection failed: {str(e)}')
        return func.HttpResponse(f'SQL connection failed: {str(e)}', status_code=500)

    # --- Call usp_log_pipeline_step ---
    try:
        cursor.execute(
            "EXEC pr.usp_log_pipeline_step %s, %s, %s, %s, %s, %s, %s, %s, %s",
            (
                run_id,
                payroll_date,
                'SAGE_VI_JOB',
                status,
                None,       # loc_id — not applicable for VI jobs
                None,       # report_type — not applicable for VI jobs
                None,       # row_count — not applicable for VI jobs
                None,       # file_name — not applicable for VI jobs
                message
            )
        )
        conn.commit()
    except Exception as e:
        logging.error(f'usp_log_pipeline_step failed: {str(e)}')
        return func.HttpResponse(f'Audit log failed: {str(e)}', status_code=500)
    finally:
        conn.close()

    logging.info(f'VI job result logged: {job_code} {status}')
    return func.HttpResponse(f'OK: {job_code} {status} logged', status_code=200)