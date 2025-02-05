import logging, os, json
import pandas as pd
import azure.functions as func
from azure.storage.blob import ContainerClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    def format_date(s):
        m = s[:s.find('/')]
        d = s[s.find('/')+1:s.rfind('/')]
        y = s[s.rfind('/')+1:]
        return y + '/' + m + '/' + d

    xlsURI = req.params.get("uri")
    csvSAS = os.environ['DATALAKE_SAS']
    emp_csvURI = os.environ['DATALAKE_EMPLOYEE_DATA_URL']
    rop_csvURI = os.environ['DATALAKE_ROP_DATA_URL']

    xls_file = xlsURI[xlsURI.find('xlsx') + 4:]
    csv_file = xls_file[1:-5] + '.csv'
    
    logging.info(f'xlsURI: {xlsURI}')
    logging.info(f'xls_file: {xls_file}')
    logging.info(f'csv_file: {csv_file}')
'''
    col_names = ['header', 'empID', 'empName', 'jobCodeID', 'clock_in', 'profitCenterID', 'clock_out', 'clock_period', 'report_period_hours']
    data_start = 0

    loc_dict = {
        "Aubrey's Powell": 2,
        "Sunspot": 3,
        "Aubrey's Cedar Bluff": 4,
        "Aubrey's Maryville": 5,
        "Aubrey's Hixson": 6,
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
        "Universal Pizza Co": 35,
        "Unused": 99
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
'''