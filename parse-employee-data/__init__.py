import logging, os, io
from datetime import datetime
import azure.functions as func
from azure.storage.blob import ContainerClient
import paramiko
import pymssql

# All active location IDs
ACTIVE_LOCATIONS = [2,3,4,5,6,8,9,11,12,13,14,16,17,18,19,20,21,22,23,24,25,35]

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

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Employee data sync batch started.')

    succeeded = []
    failed = []

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

    # Cleanup
    try:
        sftp.close()
        transport.close()
        conn.close()
    except:
        pass

    # Build response
    overall_success = len(failed) == 0
    response = {
        "success": overall_success,
        "locations_succeeded": succeeded,
        "locations_failed": failed
    }

    import json
    return func.HttpResponse(
        json.dumps(response),
        mimetype="application/json",
        status_code=200 if overall_success else 207
    )