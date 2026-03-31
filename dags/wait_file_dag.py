from airflow.sdk import Asset, TaskGroup, task
from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable


from datetime import datetime
import re
import os
from dotenv import load_dotenv
import pandas as pd


load_dotenv()
folder_path = os.getenv('FOLDER_PATH', '/opt/airflow/data')
filename = os.getenv('FILENAME', 'tiktok_google_play_reviews.csv')

filepath = f"{folder_path}/{filename}"

file_asset = Asset(uri=filepath)

def decide_branch():
    if os.path.getsize(filepath) == 0:
        return "log_empty_file"
    else:
        return "data_processing.fill_nulls"

def process_null():
    data = pd.read_csv(filepath)
    data.fillna("-", inplace=True)
    data.to_csv(filepath, index=False)
    
def sort_createdAt():
    data = pd.read_csv(filepath)
    data.sort_values("at", inplace=True)
    data.to_csv(filepath, index=False)
      

def clean_content():
    data = pd.read_csv(filepath)

    def clean_text(text):
        if isinstance(text, str):
            return re.sub(r"[^a-zA-Z0-9\s.,!?\'\";:()-]", "", text)
        return text

    data['content'] = data['content'].apply(clean_text)
    data.to_csv(filepath, index=False)

with DAG(
    dag_id='file_sensor_dag',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
       
    def _file_exists():
        return os.path.exists(filepath)

    wait_file = PythonSensor(
                task_id="wait_file",
                python_callable=_file_exists,
                poke_interval=10,
                mode="reschedule",
            )
    
    branch_task = BranchPythonOperator(
                task_id=f"branch_by_file_content",
                python_callable=decide_branch,
                trigger_rule=TriggerRule.ALL_SUCCESS,
            )
    
    
    with TaskGroup("data_processing") as data_processing:
        fill_nulls_task = PythonOperator(
            task_id='fill_nulls',
            python_callable=process_null,
        )
        
        sort_task = PythonOperator(
            task_id='sort_data',
            python_callable=sort_createdAt,
        )

        clean_task = PythonOperator(
            task_id='clean_content',
            python_callable=clean_content,
            outlets=[file_asset],
        )

        
        fill_nulls_task >> sort_task >> clean_task

    log_empty_file = BashOperator(
        task_id='log_empty_file',
        bash_command=f'echo "File is empty: {filename}"',
    )

    wait_file >> branch_task
    branch_task >> [log_empty_file, data_processing]
    


    