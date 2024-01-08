from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests    
import pandas as pd
from sqlalchemy import create_engine                     
from airflow.sensors.filesystem import FileSensor
from airflow.models import Connection
# from airflow.providers.papermill.operators.papermill import PapermillOperator


def print_welcome():
    print('Welcome to Airflow!')

def print_date():
    print('Today is {}'.format(datetime.today().date()))


def print_random_quote():
    response = requests.get('https://api.quotable.io/random')
    quote = response.json()['content']
    print('Quote of the date: "{}"'.format(quote))

def create_table_in_postgres():
    # Establish PostgreSQL engine using the 'postgres_default' connection
    conn_id = 'postgres_default'
    pg_conn = Connection.get_connection_from_secrets(conn_id)
    db_username = pg_conn.login
    db_password = pg_conn.password
    db_host = pg_conn.host
    db_name = pg_conn.schema

    engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}/{db_name}")

    # Table creation SQL statement
    table_name = 'fact_publications'
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            publication_id VARCHAR(255),
            submitter_id INT,
            title_id INT,
            doi VARCHAR(255),
            report_no VARCHAR(255),
            comments TEXT,
            publication_type VARCHAR(255),
            journal_ref_id INT,
            license_id INT,
            field_of_study_id INT,
            publication_year INT,
            journal_name VARCHAR(255),
            volume VARCHAR(50),
            issue VARCHAR(50),
            pages VARCHAR(100)
        )
    """

    # # Table creation SQL statement
    # table_name = 'dim_field_of_study'
    # create_table_query = f"""
    #     CREATE TABLE IF NOT EXISTS {table_name} (
    #         field_of_study_id SERIAL PRIMARY KEY,
    #         field_name VARCHAR(255)
    #     )
    # """

    table_name = 'dim_license'
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            license_id SERIAL PRIMARY KEY,
            license_type VARCHAR(255)
       )
    """  

    # Execute SQL statement to create the table
    with engine.connect() as connection:
        connection.execute(create_table_query)

    # # Execute a Jupyter Notebook as a task
    # pm_execute_task = PapermillOperator(
    #     task_id="execute_notebook_task",
    #     input_nb="/opt/airflow/dags/file/enriched_data.py",  # Replace with your .ipynb file path
    #     output_nb="/opt/airflow/dags/file/output_enriched_data.py",  # Replace with your desired output path
    # )

def upload_csv_data_to_postgres():
    # # Establish PostgreSQL engine using the 'postgres_default' connection
    conn_id = 'postgres_default'
    pg_conn = Connection.get_connection_from_secrets(conn_id)
    db_username = pg_conn.login
    db_password = pg_conn.password
    db_host = pg_conn.host
    db_name = pg_conn.schema

    engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}/{db_name}")

    # CSV file path to load data from
    csv_file_path = '/opt/airflow/dags/file/updated_data_new.csv'  # Replace with the file path to monitor

    # Read CSV file into pandas DataFrame
    df = pd.read_csv(csv_file_path)

    # Upload data to PostgreSQL table
    table_name = 'fact_publications'
    df.to_sql(table_name, con= engine, if_exists='replace', index=False)

def execute_python_script():
    # Path to the Python file you want to execute
    python_file_path = '/opt/airflow/dags/file/graph_view.ipynb'
    
    # Execute the Python script
    exec(open(python_file_path).read())

def execute_Preliminary_python_script():
    # Path to the Python file you want to execute
    python_file_path = '/opt/airflow/dags/file/Preliminary work.ipynb'
    
    # Execute the Python script
    exec(open(python_file_path).read())


with DAG(
    'welcome_dag',
    default_args={'start_date':days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
) as welcome_dag:

    print_welcome_task =PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome
    )

    print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date
    )

    print_random_quote = PythonOperator(
    task_id = 'print_random_quote',
    python_callable=print_random_quote
    )

    #set the dependencies between the tasks
    print_welcome_task >> print_date_task >> print_random_quote



# DAG definition
with DAG(
    'postgres_operations_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval=None,
    description='Perform PostgreSQL operations',
    # 'execute_python_script_dag',
    # default_args={'start_date': days_ago(1)},
    # schedule_interval=None,
    # description='Execute Python script in Airflow',
) as dag:
    

    create_table_task = PythonOperator(
        task_id='create_table_in_postgres_task',
        python_callable=create_table_in_postgres,
    )

    file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath='/opt/airflow/dags/file/updated_data_new.csv',  
        poke_interval=60,  # Check every 60 seconds
        timeout=600,  # Timeout after 600 seconds (10 minutes)
    )

    upload_csv_task = PythonOperator(
        task_id='upload_csv_to_postgres_task',
        python_callable=upload_csv_data_to_postgres,
    )

    # Set task dependencies
    create_table_task >> file_sensor_task >> upload_csv_task # DAG definition

with DAG(
    'execute_python_script_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='@once',
    catchup=False
) as dag:

    # Define a task to execute the Python script
    execute_script_task = PythonOperator(
        task_id='execute_python_script_task',
        python_callable=execute_python_script,
    )
    
    execute_script_task

with DAG(
    'execute_Preliminary_python_script_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='@once',
    catchup=False
) as dag:

    # Define a task to execute the Python script
    execute_primary_script_task = PythonOperator(
        task_id='execute_Preliminary_python_script_task',
        python_callable=execute_python_script,
    )
    
    execute_primary_script_task
   