

import os
import itertools
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from datetime import date, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from pandas_gbq import to_gbq
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)




#### CREDENCIALES AUTENTICAICON GOOGLE #### 
service_account_key_path = "AQUI CREDENCIALES"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "AQUI CREDENCIALES"

#### AUTENTICACION GA4 ####
client = BetaAnalyticsDataClient()
#### AUTENTICACION BIGQUERY ####
credentials = service_account.Credentials.from_service_account_file(
    service_account_key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
clients = bigquery.Client(credentials=credentials, project=credentials.project_id)

#### ID DE GA4 y RANGO DE FECHA ####
property_id = "PROPIEDAD GA4 CAJA"
starting_date = "yesterday"
ending_date = "yesterday"
project_id = 'PROYECTO GCP'


######### PARA UN NUEVO CLIENTE SE DEBE MODIFICAR PROPIERTY_ID, FECHAS, DIMENSONES y COLUMNAS - BIGQUERY #####################


try:

    #### API GA4 ####
    def query_data(api_response):
        dimension_headers = [header.name for header in api_response.dimension_headers]
        metric_headers = [header.name for header in api_response.metric_headers]
        dimensions = []
        metrics = []
        for i in range(len(dimension_headers)):
            dimensions.append([row.dimension_values[i].value for row in api_response.rows])
        dimensions
        for i in range(len(metric_headers)):
            metrics.append([row.metric_values[i].value for row in api_response.rows])
        headers = dimension_headers, metric_headers
        headers = list(itertools.chain.from_iterable(headers))   
        data = dimensions, metrics
        data = list(itertools.chain.from_iterable(data))
        df = pd.DataFrame(data)
        df = df.transpose()
        df.columns = headers
        return df
    
    
    
 ######################################################################## DIMENSION Y METRICAS TRAFICO SITIO ########################################################################
    
    trafico = [
        "date",
        "source",
        "sourceMedium",
        "campaignName",
        "eventName"
        ]
    
    metricas_trafico = [
            "sessions",
            "totalUsers",
            "conversions",
            "totalRevenue",
            "activeUsers"]
    
    numeric_columns1 = [
            "sessions",
            "totalUsers",
            "conversions",
            "totalRevenue",
            "activeUsers"]
    

    ##### EJECUTAR API GA4 ######

    request_api = RunReportRequest(
        property=f"properties/{property_id}",
    
        dimensions = [Dimension(name=name) for name in trafico],
    
        metrics = [Metric(name=name) for name in metricas_trafico],
        date_ranges=[DateRange(start_date=starting_date, end_date=ending_date)],
        limit = '1000000'
        )
    
    response = client.run_report(request_api)
    
    
    ##### EXTRACION DE LA DATA DE GA4 ######

    campanas_df = pd.DataFrame(query_data(response))
    campanas_df[numeric_columns1] = campanas_df[numeric_columns1].apply(pd.to_numeric) 
    campanas_df['date'] = pd.to_datetime(campanas_df['date'], format='%Y%m%d')
    campanas_df = campanas_df.sort_values(by='date')
    campanas_df['date'] = campanas_df['date'].dt.strftime('%Y-%m-%d')
    
    ##### ENVIO A BIGQUERY ######
    to_gbq(campanas_df, destination_table= "DESTINO" , project_id= project_id, if_exists='append', credentials= credentials)
    
    
    
    
    
    
    
 ######################################################################## DIMENSION Y METRICAS CAMPAÑA ########################################################################

    performance = [
        "date",
        "googleAdsCampaignName"
        ]
    
    metricas_performance = [
            "advertiserAdCost",
            "advertiserAdImpressions",
            "advertiserAdCostPerClick",
            "advertiserAdClicks",
            "totalAdRevenue"
        ]
    
    numeric_columns2 = [
            "advertiserAdCost",
            "advertiserAdImpressions",
            "advertiserAdCostPerClick",
            "advertiserAdClicks",
            "totalAdRevenue"
        ]
    
        
    ##### EJECUTAR API GA4 ######
    
    request_api_t = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions = [Dimension(name=name) for name in performance],
        metrics = [Metric(name=name) for name in metricas_performance],
        date_ranges=[DateRange(start_date=starting_date, end_date=ending_date)],        
        limit = '1000000'         
        )
    response_t = client.run_report(request_api_t)
    
    ##### EXTRACION DE LA DATA DE GA4 ######

    trafico = pd.DataFrame(query_data(response_t))
    trafico[numeric_columns2] = trafico[numeric_columns2].apply(pd.to_numeric)
    trafico['date'] = pd.to_datetime(trafico['date'], format='%Y%m%d')
    trafico = trafico.sort_values(by='date')
    trafico['date'] = trafico['date'].dt.strftime('%Y-%m-%d')
    
    ##### ENVIO A BIGQUERY ######
    to_gbq(trafico, destination_table= "DESTINO" ,  project_id= project_id, if_exists='append', credentials= credentials)
    


    print("Funciono bien")

except Exception as e: 
    print("Fallo la carga")
    print(e)      
    
