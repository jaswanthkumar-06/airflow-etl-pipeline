from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sqlite3
import os

DATA_PATH = "/opt/airflow/data"

def extract():
    df = pd.DataFrame({
        "order_id": [1, 2, 3],
        "amount": [100, 200, 150]
    })
    df.to_csv(f"{DATA_PATH}/extracted.csv", index=False)

def transform():
    df = pd.read_csv(f"{DATA_PATH}/extracted.csv")
    df["amount_with_tax"] = df["amount"] * 1.18
    df.to_csv(f"{DATA_PATH}/transformed.csv", index=False)

def load():
    conn = sqlite3.connect(f"{DATA_PATH}/sales.db")
    df = pd.read_csv(f"{DATA_PATH}/transformed.csv")
    df.to_sql("sales", conn, if_exists="replace", index=False)
    conn.close()

with DAG(
    dag_id="simple_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    extract_task >> transform_task >> load_task