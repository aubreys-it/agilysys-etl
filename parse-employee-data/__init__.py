"""
Old code for reference, not used in current function
import logging, os

import azure.functions as func
from azure.storage.blob import ContainerClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    txtURI = req.params.get("uri")
    txtSAS = os.environ['EMP_SAS']
    emp_csvURI = os.environ['DATALAKE_EMPLOYEE_DATA_URL']
    rop_csvURI = os.environ['DATALAKE_ROP_DATA_URL']

    txt_file = txtURI[txtURI.rfind('/')+1:]
    txtURI = txtURI[:txtURI.rfind('/')]

    emp_file = txt_file[:-3] + 'csv'
    rop_file = emp_file.replace('EMP', 'ROP')

    loc_id = txt_file[9:11]

    emp_client = ContainerClient.from_container_url(txtURI + txtSAS)
    txt_blob_client = emp_client.get_blob_client(txt_file)

    if not txt_blob_client.exists():
        logging.info(f'Blob {txt_file} does not exist')
        return func.HttpResponse(
            f'{{"success": false, "loc_id": "{loc_id}", "message": "Blob {txt_file} does not exist"}}',
            mimetype="application/json"
        )

    try:
        txt_data = txt_blob_client.download_blob().readall()
        txt_data = txt_data.decode('utf-8-sig').split('\r\n')

        emp_csv_lines = []
        rop_csv_lines = []

        for line in txt_data:
            if not line.strip():
                continue

            # Get Employee Header Information
            emp_id = line[line.find(',')+1:line.find(',', line.find(',')+1)]
            emp_line = loc_id + ',' + line[:line.find('{')] + line[line.find('}')+2:]

            # Remove special characters from Card Number column
            emp_list = emp_line.split(',')
            if len(emp_list) > 21:
                emp_list[21] = ''.join([char for char in emp_list[21] if char.isdigit()])
                emp_line = ','.join(emp_list)
                emp_csv_lines.append(emp_line)

            # Get ROP Information — loc_id prepended to match new locId column
            rop_line = line[line.find('{')+1:line.find('}')].replace('$','')
            rop_values = rop_line.split(',')
            for i in range(int(len(rop_values)/4)):
                rop_csv_lines.append(f'{loc_id},{emp_id},{rop_values[i*4]},{rop_values[i*4+1]},{rop_values[i*4+2]},{rop_values[i*4+3]}')

        emp_csv = '\r\n'.join(emp_csv_lines)
        rop_csv = '\r\n'.join(rop_csv_lines)

        emp_client = ContainerClient.from_container_url(emp_csvURI + txtSAS)
        emp_client.get_blob_client(emp_file).upload_blob(emp_csv, overwrite=True)

        rop_client = ContainerClient.from_container_url(rop_csvURI + txtSAS)
        rop_client.get_blob_client(rop_file).upload_blob(rop_csv, overwrite=True)

        logging.info(f'Successfully processed loc_id {loc_id}: {len(emp_csv_lines)} employee rows, {len(rop_csv_lines)} ROP rows.')

        return func.HttpResponse(
            f'{{"success": true, "loc_id": "{loc_id}", "emp_rows": {len(emp_csv_lines)}, "rop_rows": {len(rop_csv_lines)}}}',
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f'Error processing loc_id {loc_id}: {str(e)}')
        return func.HttpResponse(
            f'{{"success": false, "loc_id": "{loc_id}", "message": "{str(e)}"}}',
            mimetype="application/json"
        )
"""

import logging, os, io
from datetime import datetime
import azure.functions as func
from azure.storage.blob import ContainerClient
import paramiko
import pymssql

# All active location IDs
ACTIVE_LOCATIONS = [2,3,4,5,6,8,9,11,12,13,14,16,17,18,19,20,21,22,23,24,25,35]
x = 1 #debug to force Github actions to run on push, can remove later

def get_sftp_client():
    transport = paramiko.Transport((os.environ['SFTP_HOST'], int(os.environ.get('SFTP_PORT', 22))))
    transport.connect(username=os.environ['SFTP_USER'], password=os.environ['SFTP_PASSWORD'])
    return paramiko.SFTPClient.from_transport(transport), transport

def process_file(txt_data: str, loc_id: int):
    """Split raw txt data into employee header and ROP CSV lines."""
    loc_id_str = str(loc_id).zfill(2)
    emp_csv_lines = []
    rop_csv_lines = []

    for line in txt_data.split('\r\n'):
        if not line.strip():
            continue

        emp_id = line[line.find(',')+1:line.find(',', line.find(',')+1)]
        emp_line = loc_id_str + ',' + line[:line.find('{')] + line[line.find('}')+2:]

        emp_list = emp_line.split(',')
        if len(emp_list) > 21:
            emp_list[21] = ''.join([char for char in emp_list[21] if char.isdigit()])
            emp_line = ','.join(emp_list)
            emp_csv_lines.append(emp_line)

        rop_line = line[line.find('{')+1:line.find('}')].replace('$','')
        rop_values = rop_line.split(',')
        for i in range(int(len(rop_values)/4)):
            rop_csv_lines.append(f'{loc_id_str},{emp_id},{rop_values[i*4]},{rop_values[i*4+1]},{rop_values[i*4+2]},{rop_values[i*4+3]}')

    return '\r\n'.join(emp_csv_lines), '\r\n'.join(rop_csv_lines)

def upload_to_blob(emp_csv: str, rop_csv: str, loc_id: int):
    """Upload employee header and ROP CSVs to blob storage."""
    today = datetime.utcnow().strftime('%Y%m%d')
    loc_id_str = str(loc_id).zfill(2)
    txt_sas = os.environ['EMP_SAS']

    emp_file = f'{today}_{loc_id_str}_EMP.csv'
    rop_file = f'{today}_{loc_id_str}_ROP.csv'

    emp_client = ContainerClient.from_container_url(os.environ['DATALAKE_EMPLOYEE_DATA_URL'] + txt_sas)
    emp_client.get_blob_client(emp_file).upload_blob(emp_csv, overwrite=True)

    rop_client = ContainerClient.from_container_url(os.environ['DATALAKE_ROP_DATA_URL'] + txt_sas)
    rop_client.get_blob_client(rop_file).upload_blob(rop_csv, overwrite=True)

def bulk_insert(loc_id: int, conn):
    """Execute BULK INSERT for employee header and ROP for a given location."""
    today = datetime.utcnow().strftime('%Y%m%d')
    loc_id_str = str(loc_id).zfill(2)

    emp_file = f'{today}_{loc_id_str}_EMP.csv'
    rop_file = f'{today}_{loc_id_str}_ROP.csv'

    cursor = conn.cursor()

    cursor.execute(f"""
        BULK INSERT ig.v_employees
        FROM '{emp_file}'
        WITH (DATA_SOURCE='IgEmployeeHeaders', FORMAT='CSV', ROWTERMINATOR='0x0A');
    """)

    cursor.execute(f"""
        BULK INSERT ig.v_employee_rop
        FROM '{rop_file}'
        WITH (DATA_SOURCE='IgEmployeeRop', FORMAT='CSV', ROWTERMINATOR='0x0A');
    """)

    conn.commit()

def run_post_insert_procs(conn):
    """Run global archive and update procs after all inserts are complete."""
    cursor = conn.cursor()
    procs = [
        'EXEC ig.usp_archive_employees_all',
        'EXEC ig.usp_archive_employee_rop_all',
        'EXEC ig.updateEmpStatus',
        'EXEC ig.update_known_sage_ids'
    ]
    for proc in procs:
        cursor.execute(proc)
        conn.commit()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Employee data sync batch started.')

    succeeded = []
    failed = []
    sftp = None
    transport = None

    try:
        conn = pymssql.connect(
            server   = os.environ['SQL_SERVER'],
            user     = os.environ['SQL_USER'],
            password = os.environ['SQL_PASSWORD'],
            database = os.environ['SQL_DATABASE']
        )
    except Exception as e:
        logging.error(f'Database connection failed: {str(e)}')
        return func.HttpResponse(
            '{"success": false, "message": "Database connection failed"}',
            mimetype="application/json",
            status_code=500
        )

    try:
        sftp, transport = get_sftp_client()
    except Exception as e:
        logging.error(f'SFTP connection failed: {str(e)}')
        return func.HttpResponse(
            '{"success": false, "message": "SFTP connection failed"}',
            mimetype="application/json",
            status_code=500
        )

    # Phase 1 — grab each file from SFTP, split, upload to blob, bulk insert
    for loc_id in ACTIVE_LOCATIONS:
        loc_id_str = str(loc_id).zfill(2)
        sftp_path = f'/Home/aubr1.ftpadmin/Export/{loc_id_str}/Emp_Exp.txt'
        try:
            with sftp.open(sftp_path, 'r') as f:
                txt_data = f.read().decode('utf-8-sig')

            emp_csv, rop_csv = process_file(txt_data, loc_id)
            upload_to_blob(emp_csv, rop_csv, loc_id)
            bulk_insert(loc_id, conn)

            succeeded.append(loc_id)
            logging.info(f'locId {loc_id}: insert complete.')

        except Exception as e:
            failed.append(loc_id)
            logging.error(f'locId {loc_id}: failed — {str(e)}')

    # Phase 2 — run global procs once all inserts are done
    proc_error = None
    if len(succeeded) > 0:
        try:
            run_post_insert_procs(conn)
            logging.info('Post-insert procs completed successfully.')
        except Exception as e:
            proc_error = str(e)
            logging.error(f'Post-insert procs failed: {proc_error}')
    else:
        proc_error = 'Skipped — no locations inserted successfully.'
        logging.warning(proc_error)

    # Cleanup
    try:
        sftp.close()
        transport.close()
        conn.close()
    except:
        pass

    # Build response
    overall_success = len(failed) == 0 and proc_error is None
    response = {
        "success": overall_success,
        "locations_succeeded": succeeded,
        "locations_failed": failed,
        "proc_error": proc_error
    }

    import json
    return func.HttpResponse(
        json.dumps(response),
        mimetype="application/json",
        status_code=200
    )