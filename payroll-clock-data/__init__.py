import logging, os, json
import pandas as pd
import azure.functions as func


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

    df.to_csv(csvURI + csvSAS)

    return func.HttpResponse(true)