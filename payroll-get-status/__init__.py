import logging, os
import azure.functions as func
import pymssql
import json
from datetime import datetime


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('payroll-get-status: processing request.')

    # --- API key authentication ---
    api_key = req.headers.get('x-functions-key') or req.params.get('code')
    if api_key != os.environ['PAYROLL_DAEMON_API_KEY']:
        return func.HttpResponse('Unauthorized', status_code=401)

    # --- Optional parameters ---
    payroll_date    = req.params.get('payroll_date')
    include_run_id  = req.params.get('include_run_id', '').lower() == 'true'

    if not payroll_date:
        payroll_date = datetime.utcnow().strftime('%Y-%m-%d')

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

    try:
        # --- Call fn_get_payroll_status ---
        cursor.execute(
            "SELECT pr.fn_get_payroll_status(%s) AS current_status;",
            (payroll_date,)
        )
        row = cursor.fetchone()
        status = row[0] if row and row[0] else 'NOT_STARTED'

        # --- Optionally fetch run_id ---
        run_id = None
        if include_run_id:
            cursor.execute(
                """
                SELECT TOP 1 CAST(run_id AS NVARCHAR(36)) AS run_id
                FROM pr.payroll_runs
                WHERE payroll_date = %s
                  AND run_id != '00000000-0000-0000-0000-000000000000'
                ORDER BY run_started DESC
                """,
                (payroll_date,)
            )
            id_row = cursor.fetchone()
            run_id = id_row[0] if id_row and id_row[0] else '00000000-0000-0000-0000-000000000000'

    except Exception as e:
        logging.error(f'Status query failed: {str(e)}')
        return func.HttpResponse(f'Status query failed: {str(e)}', status_code=500)
    finally:
        conn.close()

    # --- Return JSON if run_id requested, plain string otherwise ---
    if include_run_id:
        body = json.dumps({ 'status': status, 'run_id': run_id })
        return func.HttpResponse(body, status_code=200, mimetype='application/json')

    return func.HttpResponse(status, status_code=200)