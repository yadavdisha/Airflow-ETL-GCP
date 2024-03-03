from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator,BigQueryExecuteQueryOperator

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "info607etl-390100"
GS_PATH = "covid19data/"
BUCKET_NAME = 'info607final-dataset'
STAGING_DATASET = "covid19_staging_dataset"
PREPROCESSED_DATASET = "covid19_preprocessed_dataset"
PROCESSED_DATASET = "covid19_processed_dataset"
LOCATION = "us-central1" 

default_args = {
    'owner': 'Rohit_Annasaheb_Ragde',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': pendulum.today().subtract(days=2),
    'retry_delay': timedelta(minutes=5),
}

with DAG('CovidWarehouse', schedule=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = EmptyOperator(
        task_id='start_pipeline',
        dag=dag
    )
    
    destination_staging_table = f'{PROJECT_ID}.{STAGING_DATASET}.staging_data'
    load_staging_data = GCSToBigQueryOperator(
        task_id='load_staging_data',
        bucket=BUCKET_NAME,
        source_objects=['covid19data/country_wise_latest.csv'],
        destination_project_dataset_table=destination_staging_table,
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        allow_quoted_newlines=True,
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'Country_Region', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'Confirmed', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Deaths', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Recovered', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Active', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'New_cases', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'New_deaths', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'New_recovered', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Deaths_100_Cases', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'Recovered_100_Cases', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'Deaths_100_Recovered', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Confirmed_last_week', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'One_week_change', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'One_week_per_increase', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'WHO_Region', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        dag=dag
    )
    
    check_staging_data = BigQueryCheckOperator(
        task_id='check_staging_data',
        use_legacy_sql=False,
        location=LOCATION,
        sql=f'SELECT COUNT(*) FROM `{destination_staging_table}`',
        dag=dag
    )


    destination_preprocessed_table = f'{PROJECT_ID}.{PREPROCESSED_DATASET}.preprocessed_data'
    preprocess_data = BigQueryInsertJobOperator(
        task_id='preprocess_data',
        configuration={
            'query': {
                'query': f'''
                    CREATE OR REPLACE TABLE `{destination_preprocessed_table}` AS (
                        SELECT *
                        FROM `{destination_staging_table}`
                        WHERE Country_Region IS NOT NULL
                            AND Confirmed IS NOT NULL
                            AND Deaths IS NOT NULL
                            AND Recovered IS NOT NULL
                            AND Active IS NOT NULL
                            AND New_cases IS NOT NULL
                            AND New_deaths IS NOT NULL
                            AND New_recovered IS NOT NULL
                            AND NOT(Deaths_100_Recovered = 'inf')
                    )
                ''',
                'useLegacySql': False
            }
        },
        location=LOCATION,
        dag=dag
    )

    destination_processed_table = f'{PROJECT_ID}.{PROCESSED_DATASET}.processed_data'
    load_final_table = BigQueryInsertJobOperator(
        task_id='load_final_table',
        configuration={
            'query': {
                'query': f'''
                    CREATE OR REPLACE TABLE `{destination_processed_table}` AS (
                        SELECT  *
                        FROM `{destination_preprocessed_table}`
                    )
                ''',
                'useLegacySql': False
            }
        },
        location=LOCATION,
        dag=dag
    )

    check_final_table = BigQueryCheckOperator(
        task_id='check_final_table',
        use_legacy_sql=False,
        location=LOCATION,
        sql=f'SELECT COUNT(*) FROM `{destination_processed_table}`',
        dag=dag
    )
    
    
training_query = """
CREATE OR REPLACE MODEL covid19_processed_dataset.train_model
OPTIONS(model_type='boosted_tree_regressor', input_label_cols=['new_cases']) AS
SELECT
    Confirmed,
    Deaths,
    Recovered,
    Active,
    New_cases,
    New_deaths,
    New_recovered,
    Deaths_100_cases,
    Recovered_100_cases,
    Deaths_100_recovered,
    Confirmed_last_week,
    One_week_change,
    One_week_per_increase
FROM `info607etl-390100.covid19_processed_dataset.processed_data`
"""


train_model = BigQueryExecuteQueryOperator(
    task_id='train_model',
    sql=training_query,
    use_legacy_sql=False,
    dag=dag
)

prediction_query = """
SELECT
    Country_Region,
    (predicted_new_cases / Confirmed) * 100 AS risk_percentage
FROM ML.PREDICT(MODEL `covid19_processed_dataset.train_model`,
    (
    SELECT
        Country_Region,
        Confirmed,
        Deaths,
        Recovered,
        Active,
        New_cases,
        New_deaths,
        New_recovered,
        Deaths_100_cases,
        Recovered_100_cases,
        Deaths_100_recovered,
        Confirmed_last_week,
        One_week_change,
        One_week_per_increase
    FROM `info607etl-390100.covid19_processed_dataset.processed_data`
    )
)
"""

prediction_task = BigQueryExecuteQueryOperator(
    task_id='make_predictions',
    sql=prediction_query,
    use_legacy_sql=False,
    destination_dataset_table='info607etl-390100.covid19_processed_dataset.predictions',
    dag=dag
)

finish_pipeline = EmptyOperator(
        task_id='finish_pipeline',
        dag=dag
    )

start_pipeline >> load_staging_data
load_staging_data >> check_staging_data
check_staging_data >> preprocess_data
preprocess_data >> load_final_table
load_final_table >> check_final_table
check_final_table >> train_model
train_model >> prediction_task
prediction_task >> finish_pipeline
