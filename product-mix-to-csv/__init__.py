import logging
import json
import pandas as pd
import azure.functions as func
from azure.storage.blob import ContainerClient
import os

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    def format_date(s):
        slash_1 = s.find('/')
        slash_2 = s.find('/', s.find('/')+1)
        m = s[:slash_1]
        d = s[slash_1+1:slash_2]
        y = s[slash_2+1:]
        return y + '/' + m + '/' + d

    # xlsURI is for file origination point
    # csvURI + SAS are for file destination
    xlsURI = req.params.get("uri")
    csvSAS = os.environ['CSV_SAS']
    csvURI = os.environ['DATALAKE_URI']

    df_cols = ['header', 'itemId', 'itemName', 'itemsConsumed', 'itemsSold', 'itemsSoldPercentTotal', 'grossRevenue', 'discounts', 'netRevenue', 'netRevenuePercentTotal','avgNetRevenue']
    data_start = 0
    revenue_category = ''
    revenue_category_id = ''
    product_class = ''
    product_class_id = ''
    dropCols = [] 

    return_dict = {}

    loc_dict = {
        "Aubrey's Papermill": 9,
        "Bistro by the Tracks": 20
    }   

    file_dict = {
        2: 'powell',
        3: 'sunspot',
        4: 'cedarbluff',
        5: 'maryville',
        6: 'hixson',
        8: 'lenoircity',
        9: 'papermill',
        11: 'cleveland',
        12: 'bluetick',
        13: 'oakridge',
        14: 'strawplains',
        15: 'fieldhouse',
        16: 'greeneville',
        17: 'bristol',
        18: 'morristown',
        20: 'bistro',
        21: 'johnsoncity',
        22: 'hardinvalley',
        23: 'sevierville'
    }

    # Get header info
    if not xlsURI:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            xlsURI = req_body.get('uri')


    if xlsURI:
        # Extract container, file name, and path from URI
        xls_container_name = xlsURI[:xlsURI.find('.net') + 5 + xlsURI[xlsURI.find('.net') + 5:].find('/')]
        xls_file = xlsURI[xlsURI.find('product-mix/') + 11:]
        xls_path = xlsURI[len(xls_container_name) + 1:xlsURI.find(xls_file)]

        if xls_file.upper().endswith('.XLSX'):

            csv_file = xls_file[:-4] + '.csv'   # Destination File Name

            # Connect to BLOB storage
            xls_container = ContainerClient.from_container_url(xls_container_name)
            xls_client = xls_container.get_blob_client(xls_path + xls_file)
            xls_stream = xls_client.download_blob().content_as_bytes()

            # Store .XLS file as Dataframe
            df = pd.read_excel(xls_stream, names=df_cols)

            for i in range(len(df)):
                if isinstance(df['itemId'][i], str):
                    if df['itemId'][i].find('Processed Business Period')>=0:
                        business_period = df['itemId'][i]
                    elif df['itemId'][i].find('Store =')>=0:
                        store_info = df['itemId'][i]
                    elif df['itemId'][i].find('Grand Total')>=0:
                        data_end = i
                if data_start == 0:
                    if isinstance(df['itemId'][i], str):
                        if df['itemId'][i].find('Revenue Category')==0:
                            data_start = i

            df = df.iloc[data_start:data_end]

            s = business_period.find('Starting ') + 9
            e = business_period.find(' ', s)
            period_start = format_date(business_period[s:e])

            s = business_period.find('Ending ') + 7
            e = business_period.find(' ', s)
            period_end = format_date(business_period[s:e])

            for loc in loc_dict.keys():
                if store_info.find(loc)>=0:
                    locId = loc_dict[loc]

            df.insert(0, 'locID', locId)
            df.insert(1, 'periodStart', period_start)
            df.insert(2, 'periodEnd', period_end)
            df.insert(3, 'revenueCategory', '')
            df.insert(4, 'revenueCategoryId', '')
            df.insert(5, 'productClass', '')
            df.insert(6, 'productClassId', '')

            df= df.reset_index(drop = True)

            for i in range(len(df)):
                if isinstance(df['itemId'][i], str):
                    if df['itemId'][i].find('Revenue Category:')>=0:
                        if df['itemId'][i].find('Total')<0:
                            revenue_category = df['itemId'][i][df['itemId'][i].find(':')+1:df['itemId'][i].find('(')]
                            revenue_category_id = df['itemId'][i][df['itemId'][i].find('(')+1:df['itemId'][i].find(')')]
                            dropCols.append(i)
                    if df['itemId'][i].find('Product Class:')>=0:
                        if df['itemId'][i].find('Total')<0:
                            product_class = df['itemId'][i][df['itemId'][i].find(':')+1:df['itemId'][i].find('(')]
                            product_class_id = df['itemId'][i][df['itemId'][i].find('(')+1:df['itemId'][i].find(')')]
                            dropCols.append(i)
                    if df['itemId'][i] in ('ID', 'Revenue Category Total', 'Product Class Total'):
                        dropCols.append(i)
                    
                df.at[i, 'revenueCategory'] = revenue_category
                df.at[i, 'revenueCategoryId'] = revenue_category_id
                df.at[i, 'productClass'] = product_class
                df.at[i, 'productClassId'] = product_class_id

            df = df.drop('header', axis=1)
            df = df.drop('periodEnd', axis=1)
            df = df.loc[:, :'avgNetRevenue']
            df = df.drop(index=dropCols)
            df = df.reset_index(drop = True)



            # Create new BLOB with CSV data
            csv_container = ContainerClient.from_container_url(csvURI + '/' + file_dict[locId] + '/product-mix' + csvSAS)
            csv_client = csv_container.get_blob_client(csv_file)
            if not csv_client.exists():
                csv_client.upload_blob(data=df.to_csv(index=False, header=False, line_terminator='\r\n'))

            # Remove original BLOB
            # xls_container.delete_blob(xls_path + xls_file)

            return_dict['func_return'] = True   # Function successful
            
        else:
            return_dict['func_return'] = False  # Function not successful

    else:
        return_dict['func_return'] = False  # Function not successful
    
    return func.HttpResponse(
        json.dumps(
            return_dict
        ),
        mimetype='application/json'
    )
