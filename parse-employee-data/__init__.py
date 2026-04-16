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