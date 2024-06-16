from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import create_engine
import pandas as pd
from pandas_gbq import to_gbq
from airflow.hooks.base_hook import BaseHook
import os
from datetime import datetime
from pandas._libs.missing import NAType
import json

# Define default arguments
default_args = {
    'owner': 'tourease',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'etl_pipeline_destination_dag',
    default_args=default_args,
    description='ETL pipeline to extract data from database, transform it, and load to BigQuery',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
)

# Custom JSON encoder to handle NaTType and datetime
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient='records')
        elif isinstance(obj, NAType):
            return None
        else:
            return super().default(obj)

def extract_data(**kwargs):
    # Get connection details from Airflow UI
    connection = BaseHook.get_connection('mysql_aws')

    # Create engine using the connection details
    engine = create_engine(f'mysql+pymysql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}')
    
    # Define table names
    tables = [
        'destinations', 'categories', 'destination_facilities', 'facilities',
        'destination_addresses', 'provinces', 'cities', 'subdistricts', 'destination_media', 'admins'
    ]
    
    # Extract data from database into DataFrames
    dataframes = {}
    with engine.connect() as conn:
        for table in tables:
            df = pd.read_sql(f'SELECT * FROM {table}', conn)
            
            # Handle NaT values and convert datetime columns
            for col in df.select_dtypes(include=['datetime64']).columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Convert other datetime-like columns (object dtype)
            for col in df.select_dtypes(include=['object']).columns:
                if df[col].apply(pd.to_datetime, errors='coerce').notnull().all():
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Replace NaT with None
            df = df.where(pd.notnull(df), None)
            
            dataframes[table] = df.to_dict(orient='records')
    
    # Push the dataframes to XComs for the next task to use
    kwargs['ti'].xcom_push(key='dataframes', value=dataframes)

def transform_data(**kwargs):
    # Pull the raw data from XComs
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(key='dataframes', task_ids='extract_data')
    dataframes = {table: pd.DataFrame(data) for table, data in raw_data.items()}

    # Perform transformations
    df_destinations = dataframes['destinations']
    df_categories = dataframes['categories']
    df_destination_facilities = dataframes['destination_facilities']
    df_facilities = dataframes['facilities']
    df_destination_addresses = dataframes['destination_addresses']
    df_provinces = dataframes['provinces']
    df_cities = dataframes['cities']
    df_subdistricts = dataframes['subdistricts']
    df_destination_media = dataframes['destination_media']
    df_admins = dataframes['admins']

    # Transformations
    # Facilities Dimension Table
    df_merged_facilities = pd.merge(df_destination_facilities, df_facilities, left_on='facility_id', right_on='id')
    df_dim_facilities = df_merged_facilities[['facility_id', 'name', 'url']].drop_duplicates()
    df_dim_facilities.rename(columns={'facility_id': 'id'}, inplace=True)
    df_dim_facilities.reset_index(drop=True, inplace=True)

    # Address Dimension Table
    df_merged_address = pd.merge(df_destination_addresses, df_provinces, left_on='province_id', right_on='id', suffixes=('_dest', '_prov'))
    df_merged_address = pd.merge(df_merged_address, df_cities, left_on='city_id', right_on='id', suffixes=('_prov', '_city'))
    df_merged_address = pd.merge(df_merged_address, df_subdistricts, left_on='subdistrict_id', right_on='id', suffixes=('_city', '_subd'))
    df_dim_address = df_merged_address[['id_dest', 'name_prov', 'name_city', 'name', 'street_name', 'postal_code']]
    df_dim_address.columns = ['id', 'provinces', 'cities', 'subdistricts', 'street_name', 'postal_code']

    # Destinations Dimension Table
    columns_to_drop = ['category_id', 'created_at', 'updated_at', 'deleted_at']
    df_dim_destinations = df_destinations.drop(columns=columns_to_drop)

    # Categories Dimension Table
    columns_to_drop = ['url', 'created_at', 'updated_at', 'deleted_at']
    df_dim_categories = df_categories.drop(columns=columns_to_drop)

    # Media Dimension Table
    columns_to_drop = ['destination_id', 'created_at', 'updated_at', 'deleted_at']
    df_dim_medias = df_destination_media.drop(columns=columns_to_drop)

    # Fact Table
    merged_df = pd.merge(df_destinations, df_categories, left_on='category_id', right_on='id', suffixes=('_destinations', '_categories'))
    merged_df = pd.merge(merged_df, df_destination_media, left_on='id_destinations', right_on='destination_id')
    merged_df = pd.merge(merged_df, df_destination_facilities, left_on='id_destinations', right_on='destination_id', how='left')
    merged_df = pd.merge(merged_df, df_destination_addresses, left_on='id_destinations', right_on='destination_id', how='left')
    merged_df['total_content_video'] = (merged_df['type'] == 'video').astype(int)
    merged_df['total_pendapatan'] = merged_df['entry_price'] * merged_df['visit_count']
    df_destination_fact = merged_df[['id_destinations', 'category_id', 'id_x', 'facility_id', 'id', 'total_content_video', 'total_pendapatan']]
    df_destination_fact.columns = ['destinations_id', 'categories_id', 'medias_id', 'facilities_id', 'address_id', 'total_content_video', 'total_pendapatan']
    df_destination_fact = df_destination_fact.drop_duplicates()
    df_destination_fact = df_destination_fact.sort_values(by='destinations_id')

    # Push the transformed dataframes to XComs for the next task to use
    transformed_data = {
        'dim_facilities': df_dim_facilities.to_dict(orient='records'),
        'dim_address': df_dim_address.to_dict(orient='records'),
        'dim_destinations': df_dim_destinations.to_dict(orient='records'),
        'dim_categories': df_dim_categories.to_dict(orient='records'),
        'dim_medias': df_dim_medias.to_dict(orient='records'),
        'destination_fact': df_destination_fact.to_dict(orient='records'),
        'admins': df_admins.to_dict(orient='records')
    }
    ti.xcom_push(key='transformed_data', value=transformed_data)

def load_to_bigquery(**kwargs):
    # Pull the transformed data from XComs
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_data')
    
    # Load BigQuery using service account credentials
    service_account_json = '/home/sapphire/airflow/accountKey.json'  # Path to your service account JSON file

    credentials = service_account.Credentials.from_service_account_file(service_account_json)

    # Function to load DataFrame into BigQuery
    def load_to_bq(df, table_name):
        project_id = 'capstone-de-sustaintour'
        dataset_id = 'data_warehouse_tourease'
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        to_gbq(df, table_id, project_id=project_id, if_exists='replace', credentials=credentials)

    # Iterate over DataFrame dictionary and load each to BigQuery
    for table_name, data in transformed_data.items():
        df = pd.DataFrame(data)
        load_to_bq(df, table_name)

    print("All dataframes loaded to BigQuery successfully.")

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task