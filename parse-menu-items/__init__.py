import logging, os
import re
import azure.functions as func
from azure.storage.blob import ContainerClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    txtURI = req.params.get("uri")
    txtSAS = os.environ['EMP_SAS']
    mi_csvURI = os.environ['DATALAKE_MI_HEADER_URL']
    miPriceLevels_csvURI = os.environ['DATALAKE_MI_PRICE_LEVELS_URL']
    miSkuCodes_csvURI = os.environ['DATALAKE_MI_SKU_CODES_URL']
    miChoiceGroups_csvURI = os.environ['DATALAKE_MI_CHOICE GROUPS_URL']
    miPrinters_csvURI = os.environ['DATALAKE_MI_PRINTERS_URL']
    miStorePriceLevels_csvURI = os.environ['DATALAKE_MI_STORE_PRICE_LEVELS_URL']

    txt_file = txtURI[txtURI.rfind('/')+1:]
    txtURI = txtURI[:txtURI.rfind('/')]
    mi_file = txt_file[:-3] + 'csv'
    priceLevels_file = mi_file.replace('MIHEADER', 'MIPRICELEVELS')
    skuCodesFile = mi_file.replace('MIHEADER', 'MISKUCODES')
    choiceGroupsFile = mi_file.replace('MIHEADER', 'MICHOICEGROUPS')
    printersFile = mi_file.replace('MIHEADER', 'MIPRINTERS')
    storePriceLevelsFile = mi_file.replace('MIHEADER', 'MISTOREPRICELEVELS')

    mi_client = ContainerClient.from_container_url(txtURI + txtSAS)
    txt_blob_client = mi_client.get_blob_client(txt_file)

    if not txt_blob_client.exists():
        logging.info(f'Blob {txt_file} does not exist')
        return func.HttpResponse('False')
    else:
        txt_data = txt_blob_client.download_blob().readall()
        txt_data = txt_data.decode('utf-8').split('\r\n')

        loc_id = txt_file[9:11]

        mi_csv_lines = []
        priceLevels_csv_lines = []
        skuCodes_csv_lines = []
        choiceGroups_csv_lines = []
        printers_csv_lines = []
        storePriceLevels_csv_lines = []

        for line in txt_data:
            #Get Menu Item Header Information
            item_id = line[line.find(',')+1:line.find(',', line.find(',')+1)]
            mi_line = loc_id + ',' + re.sub("{.*?}", "", line)
            mi_csv_lines.append(mi_line)

            #Get Price Level Information
            openBracketPos = line.find('{')
            closeBracketPos = line.find('}')
            price_level_line = line[openBracketPos+1:closeBracketPos].replace('$','')
            price_level_values = price_level_line.split(',')
            for i in range(int(len(price_level_values)/2)):
                priceLevels_csv_lines.append(f'{loc_id},{item_id},{price_level_values[i*2]},{price_level_values[i*2+1]}')

            #Get SKU Codes Information
            openBracketPos = line.find('{', closeBracketPos)
            closeBracketPos = line.find('}', openBracketPos)
            sku_code_line = line[openBracketPos+1:closeBracketPos]
            sku_code_values = sku_code_line.split(',')
            for i in range(int(len(sku_code_values)/2)):
                skuCodes_csv_lines.append(f'{loc_id},{item_id},{sku_code_values[i*2]},{sku_code_values[i*2+1]}')

            #Get Choice Groups Information
            openBracketPos = line.find('{', closeBracketPos)
            closeBracketPos = line.find('}', openBracketPos)
            choice_group_line = line[openBracketPos+1:closeBracketPos]
            choice_group_values = choice_group_line.split(',')
            for i in range(int(len(choice_group_values)/2)):
                choiceGroups_csv_lines.append(f'{loc_id},{item_id},{choice_group_values[i*2]},{choice_group_values[i*2+1]}')

            #Get Kitchen Printer Information
            openBracketPos = line.find('{', closeBracketPos)
            closeBracketPos = line.find('}', openBracketPos)
            kp_line = line[openBracketPos+1:closeBracketPos]
            kp_values = kp_line.split(',')
            for i in range(int(len(kp_values)/2)):
                printers_csv_lines.append(f'{loc_id},{item_id},{kp_values[i*2]},{kp_values[i*2+1]}')

           #Get Store Price Level Information
            openBracketPos = line.find('{', closeBracketPos)
            closeBracketPos = line.find('}', openBracketPos)
            store_price_line = line[openBracketPos+1:closeBracketPos]
            store_price_values = store_price_line.split(',')
            for i in range(int(len(store_price_values)/2)):
                storePriceLevels_csv_lines.append(f'{loc_id},{item_id},{store_price_values[i*2]},{store_price_values[i*2+1]}')
 

        mi_csv = '\r\n'.join(mi_csv_lines)
        priceLevel_csv = '\r\n'.join(priceLevels_csv_lines)
        skuCodes_csv = '\r\n'.join(skuCodes_csv_lines)
        choiceGroups_csv = '\r\n'.join(choiceGroups_csv_lines)
        printers_csv = '\r\n'.join(printers_csv_lines)
        storePriceLevels_csv = '\r\n'.join(storePriceLevels_csv_lines)

        mi_client = ContainerClient.from_container_url(mi_csvURI + txtSAS)
        mi_client.get_blob_client(mi_file).upload_blob(mi_csv)

        mi_client = ContainerClient.from_container_url(miPriceLevels_csvURI + txtSAS)
        mi_client.get_blob_client(priceLevels_file).upload_blob(priceLevel_csv)

        mi_client = ContainerClient.from_container_url(miSkuCodes_csvURI + txtSAS)
        mi_client.get_blob_client(skuCodesFile).upload_blob(skuCodes_csv)

        mi_client = ContainerClient.from_container_url(miChoiceGroups_csvURI + txtSAS)
        mi_client.get_blob_client(choiceGroupsFile).upload_blob(choiceGroups_csv)

        mi_client = ContainerClient.from_container_url(miPrinters_csvURI + txtSAS)
        mi_client.get_blob_client(printersFile).upload_blob(printers_csv)

        mi_client = ContainerClient.from_container_url(miStorePriceLevels_csvURI + txtSAS)
        mi_client.get_blob_client(storePriceLevelsFile).upload_blob(storePriceLevels_csv)

        return func.HttpResponse('True')