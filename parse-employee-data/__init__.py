import logging, os, json
import pandas as pd
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

    logging.info(f'txtURI: {txtURI}')
    logging.info(f'txt_file: {emp_file}')
    logging.info(f'csv_file: {rop_file}')

    emp_client = ContainerClient.from_container_url(txtURI + txtSAS)
    txt_blob_client = emp_client.get_blob_client(txt_file)

    if not txt_blob_client.exists():
        logging.info(f'Blob {txt_file} does not exist')
        return func.HttpResponse('False')
    else:
        txt_data = txt_blob_client.download_blob().readall()
        txt_data = txt_data.decode('utf-8').split('\r\n')

        emp_csv_lines = []
        rop_csv_lines = []

        for line in txt_data:
            #Get Employee Header Information
            emp_id = line[line.find(',')+1:line.find(',', line.find(',')+1)]
            emp_line = line[:line.find('{')] + line[line.find('}')+2:]
            emp_csv_lines.append(emp_line)

            #Get ROP Information
            rop_line = line[line.find('{')+1:line.find('}')].replace('$','')
            rop_values = rop_line.split(',')
            for i in range(int(len(rop_values)/4)):
                rop_csv_lines.append(f'{emp_id},{rop_values[i*4]},{rop_values[i*4+1]},{rop_values[i*4+2]},{rop_values[i*4+3]}')
            
        emp_csv = '\r\n'.join(emp_csv_lines)
        rop_csv = '\r\n'.join(rop_csv_lines)

        emp_client = ContainerClient.from_container_url(emp_csvURI + txtSAS)
        emp_client.get_blob_client(emp_file).upload_blob(emp_csv)
        rop_client = ContainerClient.from_container_url(rop_csvURI + txtSAS)
        rop_client.get_blob_client(rop_file).upload_blob(rop_csv)

        return func.HttpResponse('True')
    
    return func.HttpResponse('False')