import logging, os
import re
import pandas as pd
import azure.functions as func
from azure.storage.blob import ContainerClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    txtURI = req.params.get("uri")
    logging.info(f'Txt URI: {txtURI}')
    csvURI = txtURI[:txtURI.rfind('/')].replace('import', 'export')
    txt_file = txtURI[txtURI.rfind('/')+1:]
    txtURI = txtURI[:txtURI.rfind('/')]
    csv_file = txt_file.replace('.txt', '.csv')
    txtSAS = os.environ['EMP_SAS']
    
    csv_client = ContainerClient.from_container_url(txtURI + txtSAS)
    txt_blob_client = csv_client.get_blob_client(txt_file)

    if not txt_blob_client.exists():
        logging.info(f'Blob {txt_file} does not exist')
        return func.HttpResponse('False')
    else:
        txt_data = txt_blob_client.download_blob().readall()
        txt_data = txt_data.decode('utf-8').split('\r\n')

        loc_id = txt_file[9:11]
        csv_lines = []

        for line in txt_data:
            #Get Csv File Line Information
            item_id = line[line.find(',')+1:line.find(',', line.find(',')+1)]
            mi_line = loc_id + ',' + re.sub(",{.*?}", "", line).replace('$','')
            mi_line = ','.join(['' if x == '""' else x for x in mi_line.split(',')])  # Replace empty double quotes with empty strings
            mi_line = re.sub('(?<!,)"(?!,)', '""', mi_line)  # Replace single double quote with double double quotes if not surrounded by commas
            mi_line = re.sub(r'[^a-zA-Z0-9,.\-""/: ]', '', mi_line)  # Remove special characters except for allowed ones
            if mi_line.startswith('""'):
                mi_line = mi_line[1:]
            if mi_line.endswith('""'):
                mi_line = mi_line[:-1]
            mi_line = ''.join(filter(lambda char: ord(char) in range(32, 127), mi_line))  # Remove non-printable characters
            if mi_line.endswith('"'):
                mi_line = mi_line + ','
            if len(mi_line) > 5:
                # Only append if the line is not empty after processing
                csv_lines.append(mi_line)

        

        if len(csv_lines)>5:
            csv_data = '\r\n'.join(csv_lines)
            csv_client = ContainerClient.from_container_url(csvURI + txtSAS)
            csv_client.get_blob_client(csv_file).upload_blob(csv_data, overwrite=True)

        return func.HttpResponse('True')