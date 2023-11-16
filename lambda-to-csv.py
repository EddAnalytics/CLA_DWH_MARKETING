import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
import datetime
from pandas import json_normalize
import numpy as np
import logging
import boto3
import os
from io import StringIO
import time

# Constants
NOMBRE_TOPIC = 'CajaLosAndes'
S3_BUCKET = "cla-raw-storage"
S3_PATH = "youscan/"
COLUMNS_DELETE = ['language', 'starred', 'country', 'sentimentAspects']

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

s3_client = boto3.client('s3')

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)

    if 'SecretString' in response:
        secrets = json.loads(response['SecretString'])
        return secrets
    return None

def save_to_s3(df, file_name):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, sep = "|")
    full_path = os.path.join(S3_PATH, file_name)
    s3_client.put_object(Bucket=S3_BUCKET, Key=full_path, Body=csv_buffer.getvalue())
    logger.info(f"Saved {full_path} to S3 bucket {S3_BUCKET}")

def limpiar_texto(texto_html):
    if pd.notna(texto_html) and isinstance(texto_html, str):
        soup = BeautifulSoup(texto_html, 'html.parser')
        return soup.get_text()
    else:
        return texto_html

def get_clean_file_name(base_name):
    return base_name.replace(' ', '_').lower()

def lambda_handler(event, context):
    secrets = get_secret("cla/test/")
    api_key = secrets["api_key"]
    if not api_key:
        logger.error("API key not retrieved from Secrets Manager.")
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to retrieve API key from Secrets Manager.')
        }

    url = f'https://api.youscan.io/api/external/topics/?apiKey={api_key}'
    response = requests.get(url)
    
    if response.status_code == 200: 
        datos = json.loads(response.text)
        topics_data = datos["topics"]
        data_to_save = [{"id": topic["id"], "name": topic["name"]} for topic in topics_data]
        df = pd.DataFrame(data_to_save)

        df_filtrado = df[df['name'].str.contains(NOMBRE_TOPIC, case=False)]

        for index, row in df_filtrado.iterrows():
            topic_id = row['id']
            nombre_cliente = row['name']
            fecha_actual = datetime.date.today()
            rango_mes = fecha_actual - relativedelta(months=1)
            size = 50

            params = {
                "apikey": api_key,
                "from": rango_mes,
                "size": size,
                "orderBy": "seqAsc"
            }

            all_mentions = []
            columns_delete=['language', 'starred', 'country', 'sentimentAspects']
            while True:
                url = f'https://api.youscan.io/api/external/topics/{topic_id}/mentions/?'
                response = requests.get(url, params=params)

                if response.status_code == 200:
                    data_c = response.content
                    data = json.loads(data_c.decode('utf-8'))                

                    if data.get('total', 0) > 0:
                        mentions_data = data.get('mentions', [])
                        df = json_normalize(mentions_data)
                        # comprueba si existen y limpia los campos title, text, fulltext de los caracteres especiales
                        if 'title' in df.columns:
                            df['title'] = df['title'].apply(limpiar_texto)
                        if 'text' in df.columns:
                            df['text'] = df['text'].apply(limpiar_texto)
                        if 'fullText' in df.columns:
                            df['fullText'] = df['fullText'].apply(limpiar_texto)
                        
                        #elimina columnas 
                        df.drop(columns=columns_delete, inplace=True, errors='ignore')

                        all_mentions.extend(df.to_dict('records'))
                        max_seq = max(mention['seq'] for mention in mentions_data)
                        params['sinceSeq'] = max_seq
                    else:
                        break
                else:
                    logger.error(f"Error al obtener datos de la API. Código de estado: {response.status_code}")
                    break

            df = pd.DataFrame(all_mentions)
            if 'nombre cliente' not in df.columns:
                df.insert(0, 'nombre cliente', nombre_cliente)
            
            df.columns = df.columns.str.replace('.', '_')
            df.columns = df.columns.str.replace(' ', '_')
            df['id'] = df['id'].astype(str)

            df_copia = df[['id','tags', 'autoCategories', 'subjects', 'aspects', 'imageActivities', 'imageBrands', 'imageColors', 'imageObjects', 'imagePeople', 'imageScenes']].copy()
                    
            
            columns =  ['tags', 'autoCategories', 'subjects', 'aspects', 'imageActivities', 'imageBrands', 'imageColors', 'imageObjects', 'imagePeople', 'imageScenes']
            
            df.drop(columns=columns, inplace=True, errors='ignore')

            
            data = df_copia.replace({np.nan: None})

            for column in columns:
                data[column] = data[column].apply(lambda x:  eval(str(x)) if x is not None else [])
            for column in columns:
                data = data.explode(column, ignore_index = True)
            
            mentions = f'mentions_{nombre_cliente}.csv'
            
            # Save to S3:
            mentions = get_clean_file_name(f'mentions_{nombre_cliente}.csv')
            df.columns = df.columns.str.lower()
            save_to_s3(df, mentions)
            logger.info(f'Se guardó el archivo {mentions} a s3 con éxito :)')

            mentions_desanidado = get_clean_file_name(f'mentions_{nombre_cliente}_desanidado.csv')
            data.columns = data.columns.str.lower()
            save_to_s3(data, mentions_desanidado)
            logger.info(f'Se guardó el archivo {mentions_desanidado} a s3 con éxito :)')
            time.sleep(11)

    return {
        'statusCode': 200,
        'body': json.dumps('Data processed successfully and saved to S3!')
    }