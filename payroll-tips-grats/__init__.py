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
    csvURI = os.environ['DATALAKE_TIPS_GRATS_URL']

    xls_file = xlsURI[xlsURI.find('xlsx') + 4:]
    csv_file = xls_file[1:-5] + '.csv'

    logging.info(f'csv_file: {csv_file}')

    col_names=['header', 'checks', 'covers', 'grossRevenue', 'discounts', 'tips', 'grats', 'tipTransferTo', 'tipTransferFrom', 'totalEarned', 'carried', 'amtPaid', 'amtDue', 'nonPayable', 'declaredTips']
    data_start = 0
    empId = '0'
    locId = '99'

    loc_dict = {
        "Aubrey's Powell": '2',
        "Sunspot": '3',
        "Aubrey's Cedar Bluff": '4',
        "Aubrey's Maryville": '5',
        "Aubrey's Hixson": '6',
        "Aubrey's Lenoir City": '8',
        "Aubrey's Papermill": '9',
        "Aubrey's Cleveland": '11',
        "Bluetick Tavern": '12',
        "Aubrey's Oak Ridge": '13',
        "Aubrey's Strawberry Plains": '14',
        "Fieldhouse Social": '15',
        "Aubrey's Greenville": '16',
        "Aubrey's Greeneville": '16',
        "Aubrey's Bristol": '17',
        "Aubrey's Morristown": '18',
        "Marlowe": '19',
        "Bistro by the Tracks": '20',
        "Aubrey's Johnson City": '21',
        "Stefano's": '22',
        "Aubrey's Sevierville": '23',
        "Aubrey's Spring Hill": '24',
        "Aubrey's Lebanon": '25',
        "Universal Pizza Co": '35',
        "Unused": '99'
        }

    df = pd.read_excel(xlsURI, names=col_names)
    df.insert(0, 'empId', '')
    df.insert(1, 'locId', '')

    for i in range(len(df)):
        if isinstance(df.iloc[i]['header'], str):
            if df.iloc[i]['header'].find('Store =')>=0:
                store_info = df.iloc[i]['header']

                for loc in loc_dict.keys():
                    if store_info.find(loc)>=0:
                        locId = loc_dict[loc]
                
            if df.iloc[i]['header'][:7]=='Server:':
                empId=df.iloc[i]['header'][df.iloc[i]['header'].rfind('(')+1:df.iloc[i]['header'].rfind(')')]
                
        df.loc[i, 'empId'] = empId
        df.loc[i, 'locId'] = locId
        df.loc[i, 'header'] = pd.to_datetime(df.iloc[i]['header'], errors='coerce')
        df.loc[i, 'checks'] = df.iloc[i]['checks']
        df.loc[i, 'covers'] = df.iloc[i]['covers']
        df.loc[i, 'grossRevenue'] = df.iloc[i]['grossRevenue']
        df.loc[i, 'discounts'] = df.iloc[i]['discounts']
        df.loc[i, 'tips'] = df.iloc[i]['tips']
        df.loc[i, 'grats'] = df.iloc[i]['grats']
        df.loc[i, 'tipTransferTo'] = df.iloc[i]['tipTransferTo']
        df.loc[i, 'tipTransferFrom'] = df.iloc[i]['tipTransferFrom']
        df.loc[i, 'totalEarned'] = df.iloc[i]['totalEarned']
        df.loc[i, 'carried'] = df.iloc[i]['carried']
        df.loc[i, 'amtPaid'] = df.iloc[i]['amtPaid']
        df.loc[i, 'amtDue'] = df.iloc[i]['amtDue']
        df.loc[i, 'nonPayable'] = df.iloc[i]['nonPayable']
        df.loc[i, 'declaredTips'] = df.iloc[i]['declaredTips']

    df.dropna(inplace=True)
    df = df[df.empId != '']
    
    csv_container = ContainerClient.from_container_url(csvURI + csvSAS)
    csv_client = csv_container.get_blob_client(csv_file)
    if not csv_client.exists():
        csv_client.upload_blob(data=df.to_csv(index=False, header=False, line_terminator='\r\n'))
        return func.HttpResponse('True')
    else:
        return func.HttpResponse('False')