from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime
from extract_producer import data_extract, data_transform, kafka_producer, create_bucket, upload_to_s3_bucket

                            #   CREATING AIRFLOW DAGS FOR WORKFLOW MANAGEMENT

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 6),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

                                #   CREATE AND DEFINE REDSHIFT CONNECTION ID
def execute_redshift_query(sql_query):
    hook = PostgresHook(postgres_conn_id='my_redshift_conn_id')       # 'my_redshift_conn_id' is a connection ID defined in Airflow
    hook.run(sql_query)

# Instantiates the DAG
dag = DAG(
    'aliexpress_review_dags',                                               
    default_args=default_args,
    description='Team Bravo Internship',
    schedule_interval=timedelta(days=1)
)

# Define tasks for each function
data_extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=data_extract,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='upload_transformed',
    python_callable=data_transform,
    dag=dag
)

kafka_producer_task = PythonOperator(
    task_id='publish_kafka',
    python_callable=kafka_producer,
    dag=dag
)

create_bucket_task = PythonOperator(
    task_id='team-bravo-bucket',
    python_callable=create_bucket,
    dag=dag
)

upload_to_s3_bucket_task = PythonOperator(
    task_id='upload_to_S3',
    python_callable=upload_to_s3_bucket,
    dag=dag
)

run_redshift_query = PythonOperator(
    task_id='run_redshift_query',
    python_callable=execute_redshift_query,
    op_args=["SELECT * FROM AliExpress_Review LIMIT 10;"],                                         # Pass your SQL query as an argument
)

# Define task dependencies
data_extract_task >> transform_data_task >> kafka_producer_task >> create_bucket_task >> upload_to_s3_bucket_task >> run_redshift_query 





###########################################################


run_redshift_query = PythonOperator(
    task_id='run_redshift_query',
    python_callable=execute_redshift_query,
    op_args=["SELECT * FROM AliExpress_Review LIMIT 10;"],
    dag=dag,
)



# Function to execute a Redshift query
def execute_redshift_query(sql_query):
    hook = PostgresHook(postgres_conn_id='my_redshift_conn_id')
    hook.run(sql_query)