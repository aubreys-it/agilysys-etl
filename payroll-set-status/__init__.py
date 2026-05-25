import logging, os
import azure.functions as func
import pymssql
import json
from datetime import datetime

VALID_STATUSES = {
    'IN_PROGRESS', 'READY_FOR_SAGE_ID', 'READY_FOR_MASTER',
    'CREATING_MASTER', 'READY_FOR_VI', 'VI_RUNNING',
    'SAGE_EXTRACT_COMPLETE', 'PHASE_1_COMPLETE',
    'COMPLETE', 'FAILED', 'INCOMPLETE'
}

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('payroll-set-status: processing request.')

    # --- API key authentication ---
    api_key = req.headers.get('x-functions-key') or req.params.get('code')
    if api_key != os.environ['PAYROLL_DAEMON_API_KEY']:
        logging.warning('payroll-set-status: rejected request — invalid API key.')
        return func.HttpResponse('Unauthorized', status_code=401)

    # --- Parse request body ---
    try:
        body = req.get_json()
    except Exception:
        logging.warning('payroll-set-status: rejected request — invalid JSON body.')
        return func.HttpResponse('Invalid JSON body', status_code=400)

    run_id     = body.get('run_id')
    new_status = body.get('status')

    if not run_id or not new_status:
        logging.warning(
            f'payroll-set-status: rejected request — missing fields. '
            f'run_id={run_id!r}, status={new_status!r}'
        )
        return func.HttpResponse('Missing required fields: run_id, status', status_code=400)

    if new_status not in VALID_STATUSES:
        logging.warning(
            f'payroll-set-status: rejected request — unrecognized status value: {new_status!r}. '
            f'Valid values: {", ".join(sorted(VALID_STATUSES))}'
        )
        return func.HttpResponse(
            f'Invalid status value: {new_status}. '
            f'Valid values: {", ".join(sorted(VALID_STATUSES))}',
            status_code=400
        )

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

    # --- Update run_status ---
    try:
        cursor.execute(
            """
            UPDATE pr.payroll_runs
            SET run_status = %s
            WHERE run_id = %s
              AND run_id != '00000000-0000-0000-0000-000000000000'
            """,
            (new_status, run_id)
        )
        rows_affected = cursor.rowcount
        conn.commit()
    except Exception as e:
        logging.error(f'run_status update failed: {str(e)}')
        return func.HttpResponse(f'Status update failed: {str(e)}', status_code=500)
    finally:
        conn.close()

    if rows_affected == 0:
        logging.warning(
            f'payroll-set-status: no rows updated — run_id not found or is sentinel: {run_id}'
        )
        return func.HttpResponse(
            f'No rows updated — run_id not found or is sentinel: {run_id}',
            status_code=404
        )

    logging.info(f'run_status updated to {new_status} for run_id {run_id}')
    return func.HttpResponse(f'OK: run_status set to {new_status}', status_code=200)