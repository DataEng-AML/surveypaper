"""
### IDEAL Household Dataset
This DAG is to:
      (a)   Prevent filenames causing processing issues in PostgreSQL,
            by replacing dash (-) with (x) in CSV filename prior to loading into PostgreSQL as tables.
      Note: beyond this, filenames with >63 characters were manually reduced in PostgrSQL to prevent long filename failures.
            
"""
from __future__ import annotations

# [START module import]
# Required module

# To instantiate a DAG;
from airflow import DAG

from datetime import datetime, timedelta
from airflow.models import DagRun
from airflow.utils.db import provide_session, create_session
from airflow.operators.python_operator import PythonOperator
import psycopg2
from sqlalchemy import create_engine
from airflow import settings
from airflow.configuration import conf
import os
import pandas as pd


# [START instantiate_dag]
with DAG(
    "rename_read_load_csv",
    
    default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),}, 

    description="IDEAL DATA DAG VERSION2",
    schedule_interval=None,
    catchup=False,

    
) as dag:

    def rename_read_load_csv():
       
       # establish connection to the postgres database using the correct username, password and database
       engine = create_engine("postgresql+psycopg2://username:password@localhost:5432/database")
       
       # the folder location of the csv files
       folder = '/home/abc/xyz/airflow_workspace/foldername'
       
       # the list of csv in the folder
       csv_files = [csv for csv in os.listdir(folder) if csv.endswith(".csv")]
               
       # Iterate over the csv files
       for csv in csv_files:
           # tablename replace - with x due to pgsql read issues as - fails during read
           table_name = os.path.splitext(csv)[0].replace("-", "x") 

           # Create df from the csv
           csv_infolder = os.path.join(folder, csv)
           df = pd.read_csv(csv_infolder, header=None)
           #df.columns = ['datetime', 'value'] # add columns

           # Create postgres table using individual csv names
           # chunk size to help with large loads
           df.head(0).to_sql(table_name, engine, if_exists="append", chunksize = 1000, index=False) 
           

           # Populate the postgres table
           df.to_sql(table_name, engine, if_exists="append", index=False)
       
    rename_read_load_csv_task = PythonOperator(
        task_id='rename_read_load_csv',
        python_callable=rename_read_load_csv,
    )


    rename_read_load_csv_task
