from datetime import datetime
import os
import pandas as pd

from dotenv import load_dotenv

from airflow import DAG, Asset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo import UpdateOne

load_dotenv()
folder_path = os.getenv('FOLDER_PATH', '/opt/airflow/data')
filename = os.getenv('FILENAME', 'tiktok_google_play_reviews.csv')

filepath = f"{folder_path}/{filename}"

file_asset = Asset(uri=filepath)

def load_to_db():
    df = pd.read_csv(filepath)

    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()

    try:
        db = client.get_default_database()
        if db is None:
            raise Exception()
    except Exception:
        db = client['tiktok_db']

    collection = db['tiktok_reviews']
    
    collection.create_index([("reviewId", 1)], unique=True)

    records = df.where(pd.notnull(df), None).to_dict(orient='records')
    
    print(f"Loaded {len(records)} records from CSV.")
    
    if records:
        operations = (
            UpdateOne(
                { 'reviewId': r['reviewId'] }, 
                { '$set': r },                   
                upsert=True                
            ) 
            for r in records
        )
    
    

    result = collection.bulk_write(operations, ordered=False)
    
    print(f"Matched: {result.matched_count}, Upserted: {result.upserted_count}")


with DAG(
    dag_id='load_tiktok_reviews_to_mongodb',
    schedule=[file_asset],
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    load_task = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
    )
