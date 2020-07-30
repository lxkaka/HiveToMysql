#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import airflow
from airflow import DAG
from airflow.models import Variable
from dags.libs.command import get_mysql_dataset, local_tz
from dags.libs.sync import Sync, finish_generator, start_generator, task_generator

args = {
    "start_date": airflow.utils.dates.days_ago(1).replace(tzinfo=local_tz),
    "retries": 3,
}
concurrency = int(Variable.get('sync_concurrency', default_var=5))
dag = DAG(
    dag_id="run",
    concurrency=concurrency,
    max_active_runs=1,
    default_args=args,
    schedule_interval="@hourly",
)

sync_tasks = get_mysql_dataset(
    mysql_conn_id="airflow_db",
    schema="meta",
    sql=f"select * from adb_sync_task where status = 1",
)

for i, sync_task in enumerate(sync_tasks):
    start = start_generator(f'start_{i}', dag, Sync(sync_task, task_id=f'finish_{i}', dag_id=dag.dag_id))
    finish = finish_generator(f'finish_{i}', dag, Sync(sync_task))

    runs = task_generator(dag, sync_task)
    start >> runs >> finish

