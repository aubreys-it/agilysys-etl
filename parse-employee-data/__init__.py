import logging, os, io
from datetime import datetime
import azure.functions as func
from azure.storage.blob import ContainerClient
import paramiko
import pymssql

# Legacy backend locations, retrieved via SFTP from the IG host
SFTP_LOCATIONS = [2,3,4,5,6,8,9,11,12,13,14,16,17,18,19,20,21,22,23,24,25,35]

# Server 12 backend locations, retrieved via aubdatain blob storage
SERVER12_LOCATIONS = [26]

EMP_FIELD_COUNT_BASE = 26
EMP_FIELD_COUNT_WITH_PASSCODE = EMP_FIELD_COUNT_BASE + 1

def get_sftp_client():
    transport = paramiko.Transport((os.environ['SFTP_HOST'], int(os.environ.get('SFTP_PORT', 22))))
    transport.connect(username=os.environ['SFTP_USER'], password=os.environ['SFTP_PASSWORD'])
    transport.set_keepalive(30)
    return paramiko.SFTPClient.from_transport(transport), transport

SFTP_CONNECT_RETRIES = 3
SFTP_RECOVERABLE_ERRORS = (paramiko.SSHException, OSError, EOFError)

def fetch_sftp_file_with_retry(conn_holder: list, loc_id: int) -> str:
    """
    Fetch a location's employee file over SFTP, transparently reconnecting on a
    dead connection. conn_holder is a mutable [sftp, transport] pair so a
    reconnect made here is picked up by every subsequent location in the same
    invocation, not just this one.
    """
    last_exc = None
    for attempt in range(1, SFTP_CONNECT_RETRIES + 1):
        try:
            return get_sftp_employee_file(conn_holder[0], loc_id)
        except SFTP_RECOVERABLE_ERRORS as e:
            last_exc = e
            logging.warning(
                f'locId {loc_id}: SFTP fetch attempt {attempt}/{SFTP_CONNECT_RETRIES} '
                f'failed ({e}); reconnecting.'
            )
            for closeable in (conn_holder[0], conn_holder[1]):
                try:
                    if closeable:
                        closeable.close()
                except Exception:
                    pass
            try:
                conn_holder[0], conn_holder[1] = get_sftp_client()
            except Exception as reconnect_err:
                last_exc = reconnect_err
                logging.error(f'locId {loc_id}: reconnect attempt {attempt} failed ({reconnect_err}).')
    raise last_exc

def get_aubdatain_container_client():
    return ContainerClient.from_container_url(os.environ['AUBDATAIN_URL'] + os.environ['AUBDATAIN_SAS'])

def get_sftp_employee_file(sftp, loc_id: int) -> str:
    """Retrieve the raw employee export for a legacy-backend location via SFTP."""
    loc_id_str = str(loc_id).zfill(2)
    sftp_path = f'/Home/aubr1.ftpadmin/Export/{loc_id_str}/Emp_Exp.txt'
    with sftp.open(sftp_path, 'r') as f:
        return f.read().decode('utf-8-sig')

def get_blob_employee_file(loc_id: int) -> str:
    """Retrieve the raw employee export for a Server 12 location from aubdatain."""
    blob_path = f'employees/{loc_id}/Emp_Exp.txt'
    container_client = get_aubdatain_container_client()
    blob_client = container_client.get_blob_client(blob_path)
    return blob_client.download_blob().readall().decode('utf-8-sig')

def process_file(txt_data: str, loc_id: int):
    """Split raw txt data into employee header and ROP CSV lines. Identical for both backends."""
    loc_id_str = str(loc_id).zfill(2)
    emp_csv_lines = []
    rop_csv_lines = []

    for line in txt_data.split('\r\n'):
        if not line.strip():
            continue

        emp_id = line[line.find(',')+1:line.find(',', line.find(',')+1)]
        emp_line = loc_id_str + ',' + line[:line.find('{')] + line[line.find('}')+2:]
        emp_list = emp_line.split(',')

        if len(emp_list) == EMP_FIELD_COUNT_BASE:
            emp_list.append('')  # this store's export has no passcode column yet
        elif len(emp_list) != EMP_FIELD_COUNT_WITH_PASSCODE:
            logging.error(
                f'locId {loc_id}: unexpected employee field count '
                f'({len(emp_list)}) for emp_id {emp_id} — expected '
                f'{EMP_FIELD_COUNT_BASE} or {EMP_FIELD_COUNT_WITH_PASSCODE}. Skipping row.'
            )
            continue

        emp_list[21] = ''.join([char for char in emp_list[21] if char.isdigit()])
        emp_csv_lines.append(','.join(emp_list))

        rop_line = line[line.find('{')+1:line.find('}')].replace('$','')
        rop_values = rop_line.split(',')
        for i in range(int(len(rop_values)/4)):
            rop_csv_lines.append(f'{loc_id_str},{emp_id},{rop_values[i*4]},{rop_values[i*4+1]},{rop_values[i*4+2]},{rop_values[i*4+3]}')

    return '\r\n'.join(emp_csv_lines), '\r\n'.join(rop_csv_lines)

def upload_to_blob(emp_csv: str, rop_csv: str, loc_id: int):
    """Upload employee header and ROP CSVs to aubdatain, staged for BULK INSERT."""
    today = datetime.utcnow().strftime('%Y%m%d')
    loc_id_str = str(loc_id).zfill(2)
    emp_file = f'{today}_{loc_id_str}_EMP.csv'
    rop_file = f'{today}_{loc_id_str}_ROP.csv'

    container_client = get_aubdatain_container_client()

    emp_blob_path = f'employees/{loc_id}/csv/header/{emp_file}'
    container_client.get_blob_client(emp_blob_path).upload_blob(emp_csv, overwrite=True)

    rop_blob_path = f'employees/{loc_id}/csv/jobcodes/{rop_file}'
    container_client.get_blob_client(rop_blob_path).upload_blob(rop_csv, overwrite=True)


def bulk_insert(loc_id: int, conn):
    """Execute BULK INSERT for employee header and ROP for a given location, from aubdatain."""
    today = datetime.utcnow().strftime('%Y%m%d')
    loc_id_str = str(loc_id).zfill(2)
    emp_file = f'{today}_{loc_id_str}_EMP.csv'
    rop_file = f'{today}_{loc_id_str}_ROP.csv'

    emp_source, emp_path = 'AubDataInEmployee', f'employees/{loc_id}/csv/header/{emp_file}'
    rop_source, rop_path = 'AubDataInEmployee', f'employees/{loc_id}/csv/jobcodes/{rop_file}'

    cursor = conn.cursor()
    cursor.execute(f"""
        BULK INSERT ig.v_employees
        FROM '{emp_path}'
        WITH (DATA_SOURCE='{emp_source}', FORMAT='CSV', ROWTERMINATOR='0x0D0A');
    """)
    cursor.execute(f"""
        BULK INSERT ig.v_employee_rop
        FROM '{rop_path}'
        WITH (DATA_SOURCE='{rop_source}', FORMAT='CSV', ROWTERMINATOR='0x0D0A');
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

    locations = (
        [(loc_id, 'sftp') for loc_id in SFTP_LOCATIONS] +
        [(loc_id, 'server12') for loc_id in SERVER12_LOCATIONS]
    )

    # Optional ?locId=26 to test/run a single location without touching the rest of the batch.
    loc_filter = req.params.get('locId')
    if loc_filter:
        try:
            loc_filter = int(loc_filter)
        except ValueError:
            return func.HttpResponse(
                '{"success": false, "message": "locId must be an integer"}',
                mimetype="application/json",
                status_code=400
            )
        locations = [(loc_id, backend) for loc_id, backend in locations if loc_id == loc_filter]
        if not locations:
            return func.HttpResponse(
                f'{{"success": false, "message": "locId {loc_filter} is not in SFTP_LOCATIONS or SERVER12_LOCATIONS"}}',
                mimetype="application/json",
                status_code=400
            )

    # Only open an SFTP connection if this run actually touches an SFTP-backend location.
    sftp, transport = None, None
    if any(backend == 'sftp' for _, backend in locations):
        try:
            sftp, transport = get_sftp_client()
        except Exception as e:
            logging.error(f'SFTP connection failed: {str(e)}')
            try:
                conn.close()
            except:
                pass
            return func.HttpResponse(
                '{"success": false, "message": "SFTP connection failed"}',
                mimetype="application/json",
                status_code=500
            )

    conn_holder = [sftp, transport]

    for loc_id, backend in locations:
        try:
            if backend == 'sftp':
                txt_data = fetch_sftp_file_with_retry(conn_holder, loc_id)
            else:
                txt_data = get_blob_employee_file(loc_id)

            emp_csv, rop_csv = process_file(txt_data, loc_id)
            upload_to_blob(emp_csv, rop_csv, loc_id)
            bulk_insert(loc_id, conn)

            succeeded.append(loc_id)
            logging.info(f'locId {loc_id}: insert complete.')

        except Exception as e:
            failed.append(loc_id)
            logging.error(f'locId {loc_id}: failed — {str(e)}')

    # Cleanup — use conn_holder, not the original sftp/transport, since a
    # mid-run reconnect may have replaced them with a new pair.
    try:
        if conn_holder[0]:
            conn_holder[0].close()
        if conn_holder[1]:
            conn_holder[1].close()
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
