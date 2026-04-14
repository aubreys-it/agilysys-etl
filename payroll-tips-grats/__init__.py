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
                 'TIPS_GRATS', row_count, file_name, message)
            )
            conn.commit()
        except Exception as e:
            logging.error(f'Audit log failed: {str(e)}')

    # --- Pull URI from request ---
    xlsURI = req.params.get("uri")
    if not xlsURI:
        return func.HttpResponse('Missing required parameter: uri', status_code=400)

    # --- Environment variables ---
    csvSAS = os.environ['DATALAKE_SAS']
    csvURI = os.environ['DATALAKE_TIPS_GRATS_URL']

    # --- Derive file names ---
    xls_file = xlsURI[xlsURI.find('xlsx') + 4:]
    csv_file = xls_file[1:-5] + '.csv'
    logging.info(f'csv_file: {csv_file}')

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

    # --- Look up active run_id and payroll_date from database ---
    try:
        cursor.execute("""
            SELECT 
                CAST(pr.fn_get_active_run_id(CAST(GETUTCDATE() AS DATE)) AS NVARCHAR(36)) AS run_id,
                CAST(GETUTCDATE() AS DATE) AS payroll_date
        """)
        row = cursor.fetchone()
        if row and row[0]:
            run_id       = row[0]
            payroll_date = str(row[1])
        else:
            run_id       = '00000000-0000-0000-0000-000000000000'
            payroll_date = datetime.utcnow().strftime('%Y-%m-%d')
            logging.warning('No active payroll run found, using sentinel run_id')
    except Exception as e:
        run_id       = '00000000-0000-0000-0000-000000000000'
        payroll_date = datetime.utcnow().strftime('%Y-%m-%d')
        logging.error(f'run_id lookup failed: {str(e)}')

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
        'header', 'checks', 'covers', 'grossRevenue', 'discounts',
        'tips', 'grats', 'tipTransferTo', 'tipTransferFrom', 'totalEarned',
        'carried', 'amtPaid', 'amtDue', 'nonPayable', 'declaredTips'
    ]

    # --- Check if CSV already exists ---
    csv_container = ContainerClient.from_container_url(csvURI + csvSAS)
    csv_client    = csv_container.get_blob_client(csv_file)

    if csv_client.exists():
        log_audit(
            cursor, conn, run_id, payroll_date,
            step_name = 'CSV_CONVERT',
            status    = 'WARNING',
            file_name = csv_file,
            message   = f'CSV already exists, skipping conversion: {csv_file}'
        )
        return func.HttpResponse('False')

    # --- Parse the Excel file ---
    try:
        data_start = 0
        empId      = '0'
        locId      = None

        df = pd.read_excel(xlsURI, names=col_names)
        df.insert(0, 'empId', '')
        df.insert(1, 'locId', '')

        for i in range(len(df)):
            if isinstance(df.iloc[i]['header'], str):
                if df.iloc[i]['header'].find('Store =') >= 0:
                    store_info = df.iloc[i]['header']
                    for loc in loc_dict.keys():
                        if store_info.find(loc) >= 0:
                            locId = loc_dict[loc]

                if df.iloc[i]['header'][:7] == 'Server:':
                    empId = df.iloc[i]['header'][
                        df.iloc[i]['header'].rfind('(') + 1:
                        df.iloc[i]['header'].rfind(')')
                    ]

            df.loc[i, 'empId']          = empId
            df.loc[i, 'locId']          = locId if locId is not None else '99'
            df.loc[i, 'header']         = pd.to_datetime(df.iloc[i]['header'], errors='coerce')
            df.loc[i, 'checks']         = df.iloc[i]['checks']
            df.loc[i, 'covers']         = df.iloc[i]['covers']
            df.loc[i, 'grossRevenue']   = df.iloc[i]['grossRevenue']
            df.loc[i, 'discounts']      = df.iloc[i]['discounts']
            df.loc[i, 'tips']           = df.iloc[i]['tips']
            df.loc[i, 'grats']          = df.iloc[i]['grats']
            df.loc[i, 'tipTransferTo']  = df.iloc[i]['tipTransferTo']
            df.loc[i, 'tipTransferFrom']= df.iloc[i]['tipTransferFrom']
            df.loc[i, 'totalEarned']    = df.iloc[i]['totalEarned']
            df.loc[i, 'carried']        = df.iloc[i]['carried']
            df.loc[i, 'amtPaid']        = df.iloc[i]['amtPaid']
            df.loc[i, 'amtDue']         = df.iloc[i]['amtDue']
            df.loc[i, 'nonPayable']     = df.iloc[i]['nonPayable']
            df.loc[i, 'declaredTips']   = df.iloc[i]['declaredTips']

        df.dropna(inplace=True)
        df = df[df.empId != '']

        if locId is None:
            log_audit(
                cursor, conn, run_id, payroll_date,
                step_name = 'CSV_CONVERT',
                status    = 'FAILED',
                file_name = csv_file,
                message   = f'Could not determine location ID from store info'
            )
            return func.HttpResponse(
                'Could not determine location ID from store info',
                status_code=400
            )

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