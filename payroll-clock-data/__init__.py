import logging, os
import pandas as pd
import azure.functions as func
from azure.storage.blob import ContainerClient
import pymssql
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    def format_date(s):
        m = s[:s.find('/')]
        d = s[s.find('/')+1:s.rfind('/')]
        y = s[s.rfind('/')+1:]
        return y + '/' + m + '/' + d

    def log_audit(cursor, conn, run_id, payroll_date, step_name, status,
              loc_id=None, row_count=None, file_name=None, message=None):
        try:
            cursor.execute(
                "EXEC pr.usp_log_pipeline_step %s, %s, %s, %s, %s, %s, %s, %s, %s",
                (run_id, payroll_date, step_name, status, loc_id,
                'CLOCK_DETAIL', row_count, file_name, message)
            )
            conn.commit()
        except Exception as e:
            logging.error(f'Audit log failed: {str(e)}')

    # --- Pull parameters from request ---
    xlsURI      = req.params.get("uri")
    run_id      = req.params.get("run_id")
    payroll_date = req.params.get("payroll_date")

    # --- Validate required parameters ---
    if not xlsURI:
        return func.HttpResponse('Missing required parameter: uri', status_code=400)
    if not run_id:
        run_id = '00000000-0000-0000-0000-000000000000'
        logging.warning('No run_id provided, using sentinel value')
    if not payroll_date:
        payroll_date = datetime.utcnow().strftime('%Y-%m-%d')
        logging.warning('No payroll_date provided, defaulting to today')

    # --- Environment variables ---
    csvSAS  = os.environ['DATALAKE_SAS']
    csvURI  = os.environ['DATALAKE_CLOCK_DATA_URL']
    #sqlConn = os.environ['SQL_CONNECTION_STRING']

    # --- Derive file names ---
    xls_file = xlsURI[xlsURI.find('xlsx') + 4:]
    csv_file = xls_file[1:-5] + '.csv'
    logging.info(f'csv_file: {csv_file}')

    loc_dict = {
        "Aubrey's Powell": 2,
        "Sunspot": 3,
        "Aubrey's Cedar Bluff": 4,
        "Aubrey's Maryville": 5,
        "Aubrey's Hixson": 6,
        "Aubrey's Lenoir City": 8,
        "Aubrey's Papermill": 9,
        "Aubrey's Cleveland": 11,
        "Bluetick Tavern": 12,
        "Aubrey's Oak Ridge": 13,
        "Aubrey's Strawberry Plains": 14,
        "Fieldhouse Social": 15,
        "Aubrey's Greenville": 16,
        "Aubrey's Greeneville": 16,
        "Aubrey's Bristol": 17,
        "Aubrey's Morristown": 18,
        "Marlowe": 19,
        "Bistro by the Tracks": 20,
        "Aubrey's Johnson City": 21,
        "Stefano's": 22,
        "Aubrey's Sevierville": 23,
        "Aubrey's Spring Hill": 24,
        "Aubrey's Lebanon": 25,
        "Universal Pizza Co": 35,
        "Unused": 99
    }

    col_names = [
        'header', 'empID', 'empName', 'jobCodeID', 'clock_in',
        'profitCenterID', 'clock_out', 'clock_period', 'report_period_hours'
    ]

    # --- Open SQL connection ---
    # --- Open SQL connection ---
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

    # --- Check if CSV already exists ---
    csv_container = ContainerClient.from_container_url(csvURI + csvSAS)
    csv_client    = csv_container.get_blob_client(csv_file)

    if csv_client.exists():
        log_audit(
            cursor, run_id, payroll_date,
            step_name  = 'CSV_CONVERT',
            status     = 'WARNING',
            file_name  = csv_file,
            message    = f'CSV already exists, skipping conversion: {csv_file}'
        )
        return func.HttpResponse('False')

    # --- Parse the Excel file ---
    try:
        data_start = 0
        locId      = None

        df = pd.read_excel(xlsURI, names=col_names)

        for i in range(len(df)):
            if isinstance(df['header'][i], str):
                if df['header'][i].find('Processed Business Period') >= 0:
                    business_period = df['header'][i]
                elif df['header'][i].find('Store =') >= 0:
                    store_info = df['header'][i]
                elif df['header'][i] == ' Total':
                    data_end = i
            if data_start == 0:
                if isinstance(df['empID'][i], str):
                    if df['empID'][i].find('Employee ID') == 0:
                        data_start = i + 1

        df = df.iloc[data_start:data_end]
        df.drop(labels='header', axis=1, inplace=True)

        s = business_period.find('Starting ') + 9
        e = business_period.find(' ', s)
        period_start = format_date(business_period[s:e])

        s = business_period.find('Ending ') + 7
        e = business_period.find(' ', s)
        period_end = format_date(business_period[s:e])

        for loc in loc_dict.keys():
            if store_info.find(loc) >= 0:
                locId = loc_dict[loc]

        if locId is None:
            log_audit(
                cursor, run_id, payroll_date,
                step_name = 'CSV_CONVERT',
                status    = 'FAILED',
                file_name = csv_file,
                message   = f'Could not determine location ID from store info: {store_info}'
            )
            return func.HttpResponse(
                f'Could not determine location ID from: {store_info}', 
                status_code=400
            )

        df.insert(0, 'locID', locId)
        df.insert(1, 'period_start', period_start)
        df.insert(2, 'period_end', period_end)

        row_count = len(df)

    except Exception as e:
        log_audit(
            cursor, conn, run_id, payroll_date,
            step_name = 'CSV_CONVERT',
            status    = 'FAILED',
            file_name = csv_file,
            message   = f'Excel parsing failed: {str(e)}'
        )
        return func.HttpResponse(f'Excel parsing failed: {str(e)}', status_code=500)

    # --- Upload CSV to BLOB ---
    try:
        csv_client.upload_blob(
            data=df.to_csv(index=False, header=False, lineterminator='\r\n')
        )
    except Exception as e:
        log_audit(
            cursor, conn, run_id, payroll_date,
            step_name = 'CSV_CONVERT',
            status    = 'FAILED',
            loc_id    = locId,
            file_name = csv_file,
            message   = f'CSV upload to BLOB failed: {str(e)}'
        )
        return func.HttpResponse(f'CSV upload failed: {str(e)}', status_code=500)

    # --- Log success ---
    log_audit(
        cursor, conn, run_id, payroll_date,
        step_name = 'CSV_CONVERT',
        status    = 'SUCCESS',
        loc_id    = locId,
        row_count = row_count,
        file_name = csv_file
    )

    return func.HttpResponse('True')