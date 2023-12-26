from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests    
import pandas as pd
from sqlalchemy import create_engine                     


def print_welcome():
    print('Welcome to Airflow!')

def print_date():
    print('Today is {}'.format(datetime.today().date()))


def print_random_quote():
    response = requests.get('https://api.quotable.io/random')
    quote = response.json()['content']
    print('Quote of the date: "{}"'.format(quote))

dag=DAG(
    'welcome_dag',
    default_args={'start_date':days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)

print_welcome_task =PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,
    dag=dag
)

print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag
)

print_random_quote = PythonOperator(
    task_id = 'print_random_quote',
    python_callable=print_random_quote,
    dag=dag
)

#set the dependencies between the tasks

print_welcome_task >> print_date_task >> print_random_quote


# def load_csv_to_mssql():
#     # MSSQL Connection Details
#     db_username = 'root'
#     db_password = 'root'
#     db_host = '5432'
#     db_name = 'test_db'

#     engine = create_engine(f"mssql+pyodbc://{db_username}:{db_password}@{db_host}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server")

#     table_name = 'fact_publications'

#     csv_file_path = 'updated_data_new.csv'  # Update with the correct path

#     with engine.connect() as connection:
#         connection.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (publication_id VARCHAR(255), submitter_id INT, title_id INT, doi VARCHAR(255), report_no VARCHAR(255), comments TEXT, publication_type VARCHAR(255), journal_ref_id INT, license_id INT, field_of_study_id INT, publication_year INT, journal_name VARCHAR(255), volume VARCHAR(50), issue VARCHAR(50), pages VARCHAR(100))")
#         connection.execute(f"COPY {table_name} FROM '{csv_file_path}' WITH (FORMAT csv)")

# # Define Airflow DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 12, 26),
#     'retries': 1,
# }

# dag = DAG('csv_to_mssql_dag',
#           default_args=default_args,
#           description='Load CSV data into MSSQL',
#           schedule_interval=None,
# )

# load_csv_task = PythonOperator(
#     task_id='load_csv_to_mssql_task',
#     python_callable=load_csv_to_mssql,
#     dag=dag,
# )

# dag=DAG(
#     'load_to_mssql',
#     default_args={'start_date':days_ago(1)},
#     schedule_interval='0 23 * * *',
#     catchup=False
# )

# print_load_task =PythonOperator(
#     task_id='load_csv_to_mssql',
#     python_callable=load_csv_to_mssql,
#     dag=dag
# )


# # set task dependencies
# print_load_task 


# def create_db_and_user():
#     # MSSQL Connection Details for admin privileges
#     db_admin_username = 'root'
#     db_admin_password = 'root'
#     db_host = '5434'
#     db_name = 'test_db'  # Connecting to the master database to execute admin commands

#     engine_admin = create_engine(f"mssql+pyodbc://{db_admin_username}:{db_admin_password}@{db_host}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server")

#     # Create the airflow database
#     with engine_admin.connect() as connection:
#         connection.execute("CREATE DATABASE airflow")
#         connection.execute("ALTER DATABASE airflow SET READ_COMMITTED_SNAPSHOT ON")

#     # Create the airflow user
#     with engine_admin.connect() as connection:
#         connection.execute("CREATE LOGIN airflow_user WITH PASSWORD='airflow_pass123%'")
#         connection.execute("USE airflow")
#         connection.execute("CREATE USER airflow_user FROM LOGIN airflow_user")
#         connection.execute("GRANT ALL PRIVILEGES ON DATABASE::airflow TO airflow_user")

# # Define Airflow DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 12, 26),
#     'retries': 1,
# }

# dag = DAG('create_db_user_dag',
#           default_args=default_args,
#           description='Create Airflow database and user for MSSQL',
#           schedule_interval=None,
# )

# create_db_user_task = PythonOperator(
#     task_id='create_db_user_task',
#     python_callable=create_db_and_user,
#     dag=dag,
# )

# # Set task dependencies
# create_db_user_task  # Define dependencies here