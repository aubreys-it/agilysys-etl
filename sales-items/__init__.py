import logging, os, json
import pandas as pd
import azure.functions as func
from azure.storage.blob import ContainerClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    csvInURI = req.params.get("uri")
    csvSAS = os.environ['DATALAKE_SAS']
    csvOutURI = os.environ['DATALAKE_ITEM_SALES_DATA']
    csvFileName = csvInURI[csvInURI.rfind("/")+1:]

    cashCols = [
        'GA_Account_GA_Balance',
        'Check_Level_Data_With_Item_Item_Void_Amount',
        'Check_Level_Data_With_Item_Consumed_Cost',
        'Check_Level_Data_With_Item_Consumed_Weight',
        'Check_Level_Data_With_Item_Gross_Revenue',
        'Check_Level_Data_With_Item_Discount',
        'Check_Level_Data_With_Item_Net_Revenue',
        'Check_Level_Data_With_Item_Lost_Revenue',
        'Check_Level_Data_With_Item_Retail_Value',
        'Check_Level_Data_With_Item_Tax_Amount',
        'Check_Level_Data_With_Item_Revenue_Weight',
        'Check_Level_Data_With_Item_Revenue_Cost'
        ]

    dropCols = [
        'Total',
        'Total.1',
        'Total.2',
        'Total.3',
        'Total.4',
        'Total.5',
        'Total.6',
        'Total.7',
        'Total.8',
        'Total.9',
        'Total.10',
        'Total.11',
        'Total.12',
        'Total.13'
        ]
    
    locId = 99

    loc_dict = {
        "Aubrey's Powell": 2,
        "Sunspot": 3,
        "Aubrey's Cedar Bluff": 4,
        "Aubrey's Maryville": 5,
        "Aubrey's Hixson": 6,
        "Aubrey's Lenoir City": 8,
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
        "Aubrey's Spring Hill": 24,
        "Aubrey's Lebanon": 25,
        "Universal Pizza Co": 35
        }

    df = pd.read_csv(csvInURI, low_memory=False)
    df.insert(0, 'locId', '')
    df['locId'] = df['Location_Enterprise'].map(loc_dict).fillna(locId)

    for col in dropCols:
        df.drop(labels=col, axis=1, inplace=True)

    df=df.map(lambda x: x.replace('$', '').replace(',', '') if isinstance(x, str) else x)
    
    for col in df.columns:
        if col.find("_Id") != -1:
            df[col] = df[col].astype('Int64')
        if col in cashCols:
            df[col] = df[col].dropna().astype(str)
            df[col] = pd.to_numeric(df[col].str.replace(r'\((.*?)\)', r'-\1', regex=True))

    csv_container = ContainerClient.from_container_url(csvOutURI + csvSAS)
    csv_client = csv_container.get_blob_client(csvFileName)
    if not csv_client.exists():
        csv_client.upload_blob(data=df.to_csv(index=False, header=False))
        return func.HttpResponse('True')
    else:
        return func.HttpResponse('False')