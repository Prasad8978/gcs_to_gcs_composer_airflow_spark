from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime

PROJECT_ID = 'rare-tome-458105-n0'
REGION = 'us-central1'
CLUSTER_NAME = 'cluster-abc'
GCS_SCRIPT_PATH = "gs://comp-airflow-bucket/spark_scripts/gcs_to_gcs_spark_script.py"

PYSPARK_JOB = {
    'reference' : {'project_id' : PROJECT_ID},
    'placement' : {'cluster_name' : CLUSTER_NAME},
    'pyspark_job' : {'main_python_file_uri' : GCS_SCRIPT_PATH}
}

with models.DAG(
    dag_id = "run_pyspark_on_dataproc",
    schedule_interval = None,
    start_date = datetime(2025, 1, 1),
    catchup = False
) as dag:
    
    submit_job = DataprocSubmitJobOperator(
        task_id = "submit_pyspark_job",
        job = PYSPARK_JOB,
        region = REGION,
        project_id = PROJECT_ID
    )