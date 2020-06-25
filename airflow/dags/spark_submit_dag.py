import configparser
import logging
from pathlib import Path

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflowlib import emr_lib as emr

import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

config = configparser.ConfigParser()
config.read(Path(__file__).with_name("dwh.cfg"))

default_args = {
    'owner': 'dre',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 17),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('spark-submit-emr', concurrency=8, catchup=False, schedule_interval="0 0 * * *", default_args=default_args)
region = "us-west-2"

emr.client(region_name=region, aws_credentials_id="aws_credentials")


# Creates an EMR cluster
def create_emr(**kwargs):
    """
    Creates the emr cluster
    @param kwargs: the kwargs are passed
    @return: the id of the created cluster
    """
    cluster_id = emr.create_cluster(region_name=region, cluster_name='covid_flights_cluster', num_core_nodes=2)
    return cluster_id


# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    """
    Waits for the completion of the emr cluster
    @param kwargs: the kwargs are passed
    """
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)


# Terminates the EMR cluster
def terminate_emr(**kwargs):
    """
    Kills the emr cluster provided by the xcom variable for the create_cluster task
    @param kwargs: the kwargs are passed
    """
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)


def load_covid(**kwargs):
    """
    Submits the spark application to the cluster and runs the covid job
    @param kwargs: the kwargs are passed
    """
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    batch_response = emr.create_spark_batch(cluster_dns,
                                            '{}/spark/main.py'.format(config.get("S3", "PROJECT_PATH")),
                                            ['{}/spark/jobs.zip'.format(config.get("S3", "PROJECT_PATH")),
                                             '{}/spark/libs.zip'.format(config.get("S3", "PROJECT_PATH"))],
                                            ['{}/dl.cfg'.format(config.get("S3", "PROJECT_PATH"))],
                                            ['--job', 'load_covid',
                                             '--input-data', '{}'.format(config.get("S3", "INPUT_OUTPUT_DATA")),
                                             '--output-data', '{}'.format(config.get("S3", "INPUT_OUTPUT_DATA"))])
    batch_id = batch_response.json()['id']
    emr.track_statement_progress(cluster_dns, batch_id)


def load_flights(**kwargs):
    """
      Submits the spark application to the cluster and runs the flights job
      @param kwargs: the kwargs are passed
    """
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    batch_response = emr.create_spark_batch(cluster_dns,
                                            '{}/spark/main.py'.format(config.get("S3", "PROJECT_PATH")),
                                            ['{}/spark/jobs.zip'.format(config.get("S3", "PROJECT_PATH")),
                                             '{}/spark/libs.zip'.format(config.get("S3", "PROJECT_PATH"))],
                                            ['{}/dl.cfg'.format(config.get("S3", "PROJECT_PATH"))],
                                            ['--job', 'load_flights',
                                             '--input-data', '{}'.format(config.get("S3", "INPUT_OUTPUT_DATA")),
                                             '--output-data', '{}'.format(config.get("S3", "INPUT_OUTPUT_DATA"))])
    batch_id = batch_response.json()['id']
    emr.track_statement_progress(cluster_dns, batch_id)


# Define the individual tasks using Python Operators

create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    provide_context=True,
    dag=dag)

load_covid = PythonOperator(
    task_id='load_covid',
    python_callable=load_covid,
    provide_context=True,
    dag=dag)

load_flights = PythonOperator(
    task_id='load_flights',
    python_callable=load_flights,
    provide_context=True,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    provide_context=True,
    dag=dag)

# construct the DAG by setting the dependencies
create_cluster >> wait_for_cluster_completion >> [load_covid, load_flights] >> terminate_cluster
