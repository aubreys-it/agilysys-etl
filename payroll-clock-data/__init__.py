import logging, os, json
import pandas as pd
import azure.functions as func
from azure.storage.blob import ContainerClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    def format_date(s):
        s = s.zfill(10)
        m = s[:2]
        d = s[3:5]
        y = s[6:]
        return y + '/' + m + '/' + d

    xlsURI = req.params.get("uri")
    csvSAS = os.environ['DATALAKE_SAS']
    csvURI = os.environ['DATALAKE_URL']

    #xls_container_name = xlsURI[:xlsURI.find('.net) + 5 + xlsURI[xlsURI.find('.net) + 5:].find('/')]
    xls_file = xlsURI[xlsURI.find('cherokee') + 8:]
    #xls_path = xlsURI[:en(xls_container_name) + 1:xlsURI.find(xls_file)]
    csv_file = xls_file[:-4] + '.csv'

    logging.info(f'csv_file: {csv_file}')

    col_names = ['header', 'empID', 'empName', 'jobCodeID', 'clock_in', 'profitCenterID', 'clock_out', 'clock_period', 'report_period_hours']
    data_start = 0

    loc_dict = {
        "Aubrey's Papermill": 9,
        "Bistro by the Tracks": 20
        }
    
    df = pd.read_excel(xlsURI, names=col_names)

    for i in range(len(df)):
        if isinstance(df['header'][i], str):
            if df['header'][i].find('Processed Business Period')>=0:
                business_period = df['header'][i]
            elif df['header'][i].find('Store =')>=0:
                store_info = df['header'][i]
            elif df['header'][i]==' Total':
                data_end = i
        if data_start == 0:
            if isinstance(df['empID'][i], str):
                if df['empID'][i].find('Employee ID')==0:
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
        if store_info.find(loc)>=0:
            locId = loc_dict[loc]

    df.insert(0, 'locID', locId)
    df.insert(1, 'period_start', period_start)
    df.insert(2, 'period_end', period_end)

    csv_container = ContainerClient.from_container_url(csvURI + csvSAS)
    csv_client = csv_container.get_blob_client(csv_file)
    if not csv_client.exists():
        csv_client.upload_blob(data=df.to_csv(index=False, header=False, line_terminator='\r\n'))
        return func.HttpResponse('True')
    else:
        return func.HttpResponse('False')