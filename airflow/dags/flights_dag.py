import configparser
import logging
from pathlib import Path

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflowlib import emr_lib as emr
from create_tables import create_table_queries, drop_table_queries, copy_table_queries

import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from airflow.operators import DataQualityOperator

default_args = {
    'owner': 'dre',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('transform_covid_flights', concurrency=8, catchup=False, schedule_interval="0 0 * * *", default_args=default_args)
region = "us-west-2"
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_operator = DummyOperator(task_id='Finish_execution', dag=dag)
wait_for_drop_completion = DummyOperator(task_id='Wait_for_drop_completion', dag=dag)
wait_for_create_completion = DummyOperator(task_id='Wait_for_create_completion', dag=dag)
wait_for_copy_completion = DummyOperator(task_id='Wait_for_copy_completion', dag=dag)
wait_for_check_completion = DummyOperator(task_id='Wait_for_check_completion', dag=dag)

emr.client(region_name=region, aws_credentials_id="aws_credentials")


def create_dynamic_drop_table(target_table, project_dag, stmt):
    """
    Create a postgres operator for dropping the table defined in the arguments

    :parameter target_table: the table to drop
    :parameter project_dag: the dag this operator will be attached
    :return the postgres operator
    """

    task = PostgresOperator(
        task_id=f"drop_{target_table}",
        postgres_conn_id="redshift",
        sql=stmt,
        dag=project_dag,
    )
    return task


def create_dynamic_create_table(target_table, project_dag, stmt):
    """
    Create a postgres operator for creating the table defined in the arguments

    :param stmt:
    :param target_table: the table to create
    :param project_dag:  the dag this operator will be attached
    :return: the postgres operator
    """
    task = PostgresOperator(
        task_id=f"create_{target_table}",
        postgres_conn_id="redshift",
        sql=stmt,
        dag=project_dag,
    )
    return task


def create_dynamic_copy_table(target_table, project_dag, stmt):
    """
    Create a postgres operator for creating the table defined in the arguments

    :param stmt:
    :param target_table: the table to create
    :param project_dag:  the dag this operator will be attached
    :return: the postgres operator
    """
    task = PostgresOperator(
        task_id=f"copy_{target_table}",
        postgres_conn_id="redshift",
        sql=stmt,
        dag=project_dag,
    )
    return task


def create_data_checks(target_table, project_dag, attr):
    run_quality_checks = DataQualityOperator(
        task_id=f'Run_data_quality_checks_{target_table}',
        dag=project_dag,
        redshift_conn_id='redshift',
        stmt=f'select count(*) from {target_table} where {attr};',
        expected_result=0,
    )
    return run_quality_checks


for table in drop_table_queries:
    drop_task = create_dynamic_drop_table(table[0], dag, table[1])
    start_operator >> drop_task
    drop_task >> wait_for_drop_completion

for table in create_table_queries:
    create_task = create_dynamic_create_table(table[0], dag, table[1])
    wait_for_drop_completion >> create_task
    create_task >> wait_for_create_completion

for table in copy_table_queries:
    copy_task = create_dynamic_copy_table(table[0], dag, table[1])
    wait_for_create_completion >> copy_task
    copy_task >> wait_for_copy_completion

data_check_covid = create_data_checks('covid', dag, 'country is null OR dateRep is null')
wait_for_copy_completion >> data_check_covid
data_check_covid >> wait_for_check_completion
data_check_flights = create_data_checks('flights', dag, 'fl_date is null OR op_unique_carrier is null OR '
                                                        'origin_airport_id  is null OR '
                                                        'dest_airport_id is null')
wait_for_copy_completion >> data_check_flights
data_check_flights >> wait_for_check_completion

run_analytics = PostgresOperator(
        task_id="analytics",
        postgres_conn_id="redshift",
        sql='SELECT * from covid',
        dag=dag,
    )

wait_for_check_completion >> run_analytics >> end_operator
