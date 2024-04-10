
"""
### IDEAL Household Dataset
This DAG is to:
	(a)	Extract any IDEAL dataset using homeid as reference, in this case.
		-1- Extract tables prefixed as home308 from PostgreSQL database
		-2- Take a subset of the data within a time interval
	(b)	Process the data - split names.
	(c) 	Perform feature extraction.
	(d) 	Send combined output to PostgreSQL.
Note: Filenames with >63 characters were manually reduced in PostgrSQL to prevent long filename failures.
	
"""
from __future__ import annotations

# [START module import]
# Required module

# This would be required to instantiate a DAG
from airflow import DAG #instantiate
from datetime import datetime, timedelta

# To obtain properties of Dag runs necessary for run analysis
from airflow.models import DagRun
from airflow.utils.db import provide_session, create_session

# Airflow operators for connection and task operations
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# To assist with database connections, use as required
import psycopg2
from sqlalchemy.orm import column_property
from sqlalchemy import create_engine
from airflow import settings
from airflow.configuration import conf

# For wotking with local directory, folder, access to files and operations
import os
import pandas as pd
from pathlib import Path
import csv
import io
from io import StringIO
import glob

# json
import json
from json import loads, dumps

###

# [START instantiate_dag]
with DAG(
    "ideal_dataset_processing",
    
    default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 10), # date in the past to ensure airflow is active to run
    'retries': 1,
    'retry_delay': timedelta(minutes=5),}, 

    description="IDEAL DATA DAG VERSION2",
    schedule_interval=None,
    catchup=False,
		
       
    
) as dag:

	# IDEAL homeid
	
	#Slice options - slice intervals for the datetime column
	start_datetime = pd.to_datetime("2017-03-01 00:00:00")
	end_datetime = pd.to_datetime("2017-03-15 00:00:00")


	def extract_combine_tables_from_pgsql():
	
		#import IDEAL_read_data as rd

		# establish connection to the postgres database, set as desired the Conn Id for example in http://localhost:8080/connection/list/
		conn = PostgresHook(postgres_conn_id="postgres_default").get_conn()
		
		cursor = conn.cursor()

		cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'home308%'")
		table_names = cursor.fetchall()

				  
		# Fetch data from each table and combine into a dataframe
		
		df = [] #to use the slice

		for table_name in table_names:
			cursor.execute(f"SELECT * FROM {table_name[0]}")
			postgres_table = cursor.fetchall()
			column_names = [desc[0] for desc in cursor.description]
			initial_df = pd.DataFrame(postgres_table, columns=column_names)
			
			# create a new column source_table and populate with the table name 
			initial_df['source_table'] = table_name[0]			
			initial_df = pd.DataFrame(initial_df)			
			
			# the first 2 columns of the data has columns 0 and 1, use suitable column names in addition to source_table
			initial_df.columns = ['datetime', 'value', 'source_table']
			
			# Prep to slice on datetime column to have a common start/end datetime as determined in #Slice options lines 107 - 109
			
			# assign datetime axis to datetime data type
			initial_df['datetime'] = pd.to_datetime(initial_df['datetime']) 
			
			# reference the chosen slice start/end datetme option
			slice_range = (initial_df['datetime'] >= start_datetime) & (initial_df['datetime'] <= end_datetime)
			
			# slice each datetime axis of the initial dataframes to the slice option
			df_slice = initial_df.loc[slice_range]
			
			df.append(df_slice)

		combined_df = pd.concat(df, ignore_index=True)
		
		# sort the df along the datetime
		combined_df = combined_df.sort_values('datetime', ascending=True)	
		
		# large number of records ==> output to be processed in chunks afterwards
		print("Number of rows, columns " + str(combined_df.shape))
		
		# to_csv local storage
		homeid='/home308'
		export_location = '/home/abc/xyz/airflow_workspace/home_export'
				
		combined_df.to_csv(f"{export_location}"f"{homeid}"f'_combined_df', index=False)	
					
		conn.commit()
		conn.close()



	def split_df_for_processing():
		
		# file location, same address as the export location
		import_location = '//home/abc/xyz/airflow_workspace/home_export/home308_combined_df'
		preprocess_df = pd.read_csv(import_location)
		preprocess_df = pd.DataFrame(preprocess_df)
		
		preprocess_df['datetime'] = pd.to_datetime(preprocess_df['datetime']) 
		
		# processing limit in this airflow set is ~30m records
		chunk_limit = 10000000 # this airflow set up fails to process more than about 50 million records at a time
		number_of_parts = (len(preprocess_df) // chunk_limit)+1
		
		
		print("start the split process")
		
		# df split process
		parts_of_df = []
		for each_part in range(number_of_parts):
			first = each_part * chunk_limit
			last = min((each_part + 1) * chunk_limit, len(preprocess_df))
			each_df = preprocess_df[first:last]
			parts_of_df.append(each_df)
		
		print("complete split process")
		#initial_df = pd.DataFrame(initial_df)
		
		# export each to the processing folder /home/rpgsbs/r06am22/airflow_workspace/home_processing
		
		# to_csv local storage
		processing_location = '/home/abc/xyz/airflow_workspace/home_processing'
		
		homeid='/home308'
		
		for each, part in enumerate(parts_of_df):
			part.to_csv(f"{processing_location}"f"{homeid}"f'_part_{each}.csv', index=False)
		
		print("split df part saved successfully")
		

			
	def split_compound_columns():
	
		split_df_location = '/home/abc/xyz/airflow_workspace/home_processing'
		
		homeid='home308'
		
		
		# empty list to accommodate the processed sdf
		split_df = []
		
		# the list of csv in the folder
		ideal_files = [csv for csv in os.listdir(split_df_location) if csv.startswith(f"{homeid}")]

		# Iterate over the csv files
		for csv in ideal_files:
			#file_name = os.path.splitext(csv)[0]
			
			# Create df from the csv 
			df = pd.read_csv(os.path.join(f"{split_df_location}", f"{csv}")) # header is present
			
			df = pd.DataFrame(df)	
			
			# assign datetime appropraitely to dtype datetime
			df['datetime'] = pd.to_datetime(df['datetime']) 
			
			# replace x in the source_table with - to retain the original file format
			# - was replaced by x to allow read from postgreSql			
			df['source_table'] = df['source_table'].str.replace('x','-')
			
			# split the source_table column and add readable column names
			df[['homehomeid','roomtyperoomid','sensorsensorid','sensorboxtype','sensortype']] = df.source_table.str.split("_", expand = True)
			df.drop('source_table', inplace=True, axis=1)
			df[['home','homeid']] = df['homehomeid'].str.extract(r'([A-Za-z]+)(\d+)', expand=True)
			df[['roomtype','roomid']] = df['roomtyperoomid'].str.extract(r'([A-Za-z]+)(\d+)', expand=True)
			df[['sensor','sensorid']] = df['sensorsensorid'].str.extract(r'([A-Za-z]+)(\d+)', expand=True)
			df.drop(['home', 'homehomeid', 'roomtyperoomid', 'sensorsensorid', 'sensor'], inplace=True, axis=1)
			
			
			
			print("df after processing source_table columns")
			print(df.head())			
			
			#rearrange columns
			preferred_order = ['datetime', 'homeid', 'roomid', 'roomtype', 'sensorid', 'sensortype', 'sensorboxtype', 'value'] 
			
			df = df[preferred_order]
			
			split_df.append(df)


		print("complete the split compound columns")
		
		# export each to the processing folder /home/rpgsbs/r06am22/airflow_workspace/home_processing
		
		# to_csv local storage
		processed_column_location = '/home/abc/xyz/airflow_workspace/home_processed_column'
		
		homeidref='/home308'
		
		for each, processed_part in enumerate(split_df):
			processed_part.to_csv(f"{processed_column_location}"f"{homeidref}"f'_processed_column_{each}.csv', index=False)
		
		print("successfully processed and saved each parts of the column splits")
		


	def combine_df_send_to_postgres_json():
	
		processed_column_location = '/home/abc/xyz/airflow_workspace/home_processed_column'
		
		homeid='home308'
		
		# empty list to accommodate the processed sdf
		each_processed_df = []
		
		for each_file in os.listdir(processed_column_location):
			if each_file.startswith(f"{homeid}"):
				file_location = os.path.join(processed_column_location, each_file)
				each_df = pd.read_csv(file_location)
				each_processed_df.append(each_df)
		
		# concat each df into one df		
		combined_processed_df = pd.concat(each_processed_df, ignore_index=True)
		
		print(combined_processed_df.head())
		
		# send the combined processed df to different repositories
		homeid='/home308'
		location = '/home/abc/xyz/airflow_workspace/home_combined'
		
		# as csv to local repository		
		combined_processed_df.to_csv(f"{location}"f"{homeid}"f'_processed_combined_df', index=False)
		
		print("combined_processed_df successfully exported as a csv")
		
		# as json to local repository		
		combined_processed_df.to_json(f"{location}"f"{homeid}"f'_processed_combined_df', orient="records")
		
		print("combined_processed_df successfully exported as a json")
		
		# to_postgres
		engine = create_engine("postgresql+psycopg2://username:password@localhost:5432/database")
		combined_processed_df.to_sql('home308_combined_processed_df', engine, if_exists="replace", index=False)
		
		print("combined_processed_df successfully exported to postgres")



        # Use the PythonOperator to execute the functions as tasks
	extract_combine_tables_from_pgsql_task = PythonOperator(
	task_id='extract_combine_tables_from_pgsql',
        python_callable=extract_combine_tables_from_pgsql,
    )
    
	split_df_for_processing_task = PythonOperator(
	task_id='split_df_for_processing',
	python_callable=split_df_for_processing,
    )
    
	split_compound_columns_task = PythonOperator(
	task_id='split_compound_columns',
	python_callable=split_compound_columns,
    )
        
	combine_df_send_to_postgres_json_task = PythonOperator(
	task_id='combine_df_send_to_postgres_json',
	python_callable=combine_df_send_to_postgres_json,
    )
    
        # Execute the tasks a desired order.
	extract_combine_tables_from_pgsql_task >> split_df_for_processing_task >> split_compound_columns_task >> combine_df_send_to_postgres_json_task
   
