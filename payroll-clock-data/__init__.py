import logging
import pandas as pd
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    s = pd.Series([1, 2, 5, 6, 8])
    return func.HttpResponse(s.to_json())