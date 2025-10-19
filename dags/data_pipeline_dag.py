# dags/data_pipeline_dag.py

from __future__ import annotations

import pendulum

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# 1. Define the default arguments for the DAG
# These are parameters that will be applied to all tasks in the DAG.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # The task will retry once upon failure.
}

# 2. Instantiate the DAG object
# This is the main container for your workflow.
with DAG(
    dag_id='automated_data_pipeline',
    default_args=default_args,
    description='A DAG to run the complete ETL data pipeline inside a Docker container.',
    schedule_interval='0 3 * * *',  # The same cron schedule: run daily at 3 AM.
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), # The date from which the DAG should start running.
    catchup=False, # If true, it would run for all past, un-run schedules. False is good for starting out.
    tags=['data-pipeline'],
) as dag:

    # 3. Define the Task using an Operator
    # We use the DockerOperator to run the image you've already built.
    run_etl_pipeline_task = DockerOperator(
        task_id='run_etl_in_container',
        image='my-data-pipeline:latest', # The name and tag of your Docker image.
        command=None, # We don't need a command, as our image's CMD instruction already has it.
        api_version='auto',
        auto_remove=True, # This cleans up the container after it finishes its run.
        docker_url="unix://var/run/docker.sock", # Tells Airflow how to connect to the Docker daemon.
        network_mode="bridge" # The default Docker network mode.
    )

    # 4. (Optional) Define Task Dependencies
    # Since we only have one task, there are no dependencies to set.
    # If we had another task, we would define the order like this:
    # run_etl_pipeline_task >> another_task
