from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.oauth2 import service_account
import pandas as pd
from sqlalchemy import create_engine
from pandas_gbq import to_gbq
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from pandas._libs.missing import NAType
import json

# Custom JSON encoder to handle NAType, datetime, and Timedelta
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
        elif isinstance(obj, pd.Timedelta):
            return str(obj)
        else:
            return super().default(obj)

# Mendefinisikan argumen default
default_args = {
    'owner': 'tourease',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Inisialisasi DAG
dag = DAG(
    'etl_pipeline_routes_dag',
    default_args=default_args,
    description='Pipeline ETL untuk ekstraksi data dari database, transformasi, dan load ke BigQuery',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
)

def extract_data(**kwargs):
    connection = BaseHook.get_connection('mysql_aws')
    engine = create_engine(f'mysql+pymysql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}')
    
    tables = ['destinations', 'routes', 'route_details', 'users']
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
    kwargs['ti'].xcom_push(key='extracted_data', value=json.dumps(dataframes, cls=CustomEncoder))

def transform_data(**kwargs):
    ti = kwargs['ti']
    extracted_data = json.loads(ti.xcom_pull(key='extracted_data', task_ids='extract_data'))
    
    destinations_df = pd.DataFrame(extracted_data['destinations'])
    routes_df = pd.DataFrame(extracted_data['routes'])
    route_details_df = pd.DataFrame(extracted_data['route_details'])
    users_df = pd.DataFrame(extracted_data['users'])

    month_mapping = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
    5: 'Mei', 6: 'Jun', 7: 'Jul', 8: 'Agu',
    9: 'Sep', 10: 'Okt', 11: 'Nov', 12: 'Des'
    }

    users_df['tahun'] = pd.to_datetime(users_df['created_at']).dt.year
    users_df['bulan'] = pd.to_datetime(users_df['created_at']).dt.month.map(month_mapping)
    users_df['tanggal'] = pd.to_datetime(users_df['created_at']).dt.day
    users_df['tanggallengkap'] = pd.to_datetime(users_df['created_at']).dt.strftime('%Y-%m-%d')
    
    dim_destinations = destinations_df[['id', 'name', 'description', 'open_time', 'close_time', 'entry_price', 'longitude', 'latitude', 'visit_count']]
    dim_routes = routes_df[['id', 'name', 'start_longitude', 'start_latitude', 'price']]
    dim_route_details = route_details_df[['id', 'longitude', 'latitude', 'duration', 'order', 'visit_start', 'visit_end']]
    dim_users = users_df[['id', 'email', 'username', 'fullname', 'phone_number', 'gender', 'city', 'province', 'tahun', 'bulan', 'tanggal', 'tanggallengkap']]
    
    dim_route_details['visit_start'] = dim_route_details['visit_start'].astype(str)
    dim_route_details['visit_end'] = dim_route_details['visit_end'].astype(str)
    
    merged_df = pd.merge(routes_df, route_details_df, left_on='id', right_on='route_id', suffixes=('_routes', '_route_details'))
    merged_df = pd.merge(merged_df, users_df, left_on='user_id', right_on='id', suffixes=('_routes', '_users'))
    merged_df = pd.merge(merged_df, destinations_df, left_on='destination_id', right_on='id', suffixes=('_routes_users', '_destinations'))
    
    routes_fact = merged_df[['id_routes', 'user_id', 'id_route_details', 'id_destinations']]
    routes_fact = routes_fact.rename(columns={"id_routes": "route_id", "id_route_details": "route_details_id", "id_destinations": "destinations_id"})
    routes_fact['user_count'] = 1
    routes_fact['route_count'] = 1
    routes_fact['destination_count'] = 1
    
    transformed_data = {
        'dim_destinations': dim_destinations.to_dict(orient='records'),
        'dim_routes': dim_routes.to_dict(orient='records'),
        'dim_route_details': dim_route_details.to_dict(orient='records'),
        'dim_users': dim_users.to_dict(orient='records'),
        'routes_fact': routes_fact.to_dict(orient='records')
    }
    
    ti.xcom_push(key='transformed_data', value=json.dumps(transformed_data, cls=CustomEncoder))

def load_to_bigquery(**kwargs):
    ti = kwargs['ti']
    transformed_data = json.loads(ti.xcom_pull(key='transformed_data', task_ids='transform_data'))
    
    credentials_path = '/home/sapphire/airflow/accountKey.json'
    project_id = 'capstone-de-sustaintour'
    dataset_id = 'data_warehouse_tourease'
    
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    def load_to_bq(df, table_name):
        table_full_id = f'{project_id}.{dataset_id}.{table_name}'
        to_gbq(df, table_full_id, project_id=project_id, if_exists='replace', credentials=credentials)
    
    table_names = ['dim_destinations', 'dim_routes', 'dim_route_details', 'dim_users', 'routes_fact']
    dataframes = [
        pd.DataFrame(transformed_data['dim_destinations']),
        pd.DataFrame(transformed_data['dim_routes']),
        pd.DataFrame(transformed_data['dim_route_details']),
        pd.DataFrame(transformed_data['dim_users']),
        pd.DataFrame(transformed_data['routes_fact'])
    ]
    
    for df, table_name in zip(dataframes, table_names):
        load_to_bq(df, table_name)
        print(f'Tabel {table_name} berhasil dimuat!')

# Mendefinisikan task
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

# Mengatur dependencies antar task
extract_task >> transform_task >> load_task