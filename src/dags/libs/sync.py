#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import multiprocessing
import os
import time
from concurrent.futures import ALL_COMPLETED, wait
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import closing

import _mysql_exceptions
import oss2
import pyarrow.orc as orc
from airflow.exceptions import AirflowSkipException
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.state import State
from dags.libs.command import get_mysql_dataset
from hooks.oss_hook import OSSHook
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

oss_bucket = OSSHook(oss_conn_id='oss_hadoop').get_conn()
file_addr = '/root/airflow'


class Sync:
    def __init__(self, sync_task, task_id=None, dag_id=None):
        self.sync_task = sync_task
        self.sync_task_id = task_id
        self.sync_task_dag_id = dag_id
        self.adb_table = sync_task['adb_table']
        self.dml_operator = sync_task['dml_operator']
        self.mysql = MySqlHook(mysql_conn_id='adb_default')
        self.log = logger

    def unfinished(self):
        dag_id = self.sync_task["dag_id"]
        task_id = self.sync_task["task_id"]
        # filter the latest dependent task
        dep_sql = f"select end_date from task_instance where dag_id='{dag_id}' and task_id='{task_id}' and state='success' order by end_date desc limit 1"
        dep_res = get_mysql_dataset(
            mysql_conn_id="airflow_emr", schema="airflow", sql=dep_sql
        )
        self.log.info(f'dependent task：{dep_sql} {dep_res}')
        # filter the latest succeeded sync task
        sql = f"select end_date from task_instance where dag_id='{self.sync_task_dag_id}' and task_id='{self.sync_task_id}' and state='success' order by end_date desc limit 1"
        res = get_mysql_dataset(mysql_conn_id="airflow_db", schema="airflow", sql=sql)
        self.log.info(f'recent task run: {sql} {res}')

        need_running = bool(dep_res and (not res or res[0]["end_date"] < dep_res[0]["end_date"]))
        if not need_running:
            return True

        # task execution log after upstream task success
        upstream_task_latest_success_time = res[0]["end_date"].strftime(
            "%Y-%m-%d %H:%M:%S") if res else '1970-01-01 00:00:00'
        task_state_sql = f"select state from task_instance where dag_id='{self.sync_task_dag_id}' and task_id='{self.sync_task_id}' and execution_date > '{upstream_task_latest_success_time}'"
        target_tasks = get_mysql_dataset(
            mysql_conn_id="airflow_db", schema="airflow", sql=task_state_sql
        )
        unfinished_tasks = [_ for _ in target_tasks if _["state"] in State.unfinished()]

        # if record unfinished > 1, means task is running
        return len(unfinished_tasks) > 1

    def mysql_run(self, table, values, columns, columns_num):
        """insert data to mysql table"""
        self.log.info(f'loading data to {table}')
        placeholders = ",".join(["%s"] * columns_num)
        sql = f'{self.dml_operator} {table} ({columns}) values({placeholders})'
        with closing(self.mysql.get_conn()) as mysql_conn:
            mysql_conn.cursor().executemany(sql, values)
            mysql_conn.commit()
            self.log.info("load data done")

    def rename_adb_table(self, table_name, new_table_name):
        """ rename mysql table """
        with closing(self.mysql.get_conn()) as mysql_conn:
            self.log.info(f"Renaming {table_name} to {new_table_name}")
            mysql_conn.cursor().execute(
                f"rename table {table_name} to {new_table_name}"
            )
            self.log.info(f"Done Renaming {table_name}")

    def create_adb_table(self, table_name, create_sql):
        """ create mysql table """
        try:
            with closing(self.mysql.get_conn()) as mysql_conn:
                self.log.info(f"Creating table {table_name}, sql: {create_sql}")
                mysql_conn.cursor().execute(create_sql)
                self.log.info(f"Done creating table {table_name}")
        except _mysql_exceptions.OperationalError as e:
            self.log.debug(e)
            # the other thread has created table
            if e.args[0] != 1050:
                raise e

    def drop_adb_table(self, table_name):
        """ delete mysql table """
        with closing(self.mysql.get_conn()) as mysql_conn:
            self.log.info(f"Dropping table {table_name}")
            drop_sql = f"drop table if exists `{table_name}`;"
            mysql_conn.cursor().execute(drop_sql)
            self.log.info(f"Done dropping table {table_name}")

    def is_adb_table_exists(self, table_name):
        with closing(self.mysql.get_conn()) as mysql_conn:
            check_sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'dm' AND table_name = '{table_name}'"
            result = mysql_conn.cursor().execute(check_sql)
            return bool(result)

    def generate_create_sql(self, hive_table, mysql_table):
        """ generate create table sql """
        hive_table_columns = self.get_mysql_dataset(
            mysql_conn_id="hivemeta_db",
            schema="hivemeta",
            sql=self.get_hive_sql(*hive_table.split(".")),
        )
        partition_columns = self.get_mysql_dataset(
            mysql_conn_id="hivemeta_db",
            schema="hivemeta",
            sql=self.get_hive_partition(*hive_table.split(".")),
        )
        create_sql = [f"create table `{mysql_table}` ("]
        for column in hive_table_columns + partition_columns:
            column_name, column_type, column_comment = (
                column["column_name"],
                self.convert_column_type(column["column_type"]),
                column["column_comment"],
            )
            if column_comment:
                create_sql.append(
                    f"`{column_name}` {column_type} COMMENT '{column_comment}',"
                )
            else:
                create_sql.append(f"`{column_name}` {column_type},")
        extra_sql = self.sync_task["extra_sql"]
        if extra_sql:
            if not extra_sql.startswith('primary key'):
                create_sql[-1] = create_sql[-1].rstrip(",")
            create_sql.append(extra_sql)
        else:
            create_sql[-1] = create_sql[-1].rstrip(",")
            create_sql.append(") INDEX_ALL='Y';")
        return "".join(create_sql)

    def get_target_fields(self, hive_table):
        hive_table_columns = self.get_mysql_dataset(
            mysql_conn_id="hivemeta_db",
            schema="hivemeta",
            sql=self.get_hive_sql(*hive_table.split(".")),
        )
        partition_columns = self.get_mysql_dataset(
            mysql_conn_id="hivemeta_db",
            schema="hivemeta",
            sql=self.get_hive_partition(*hive_table.split(".")),
        )
        return ", ".join(
            [
                f'`{column["column_name"]}`'
                for column in hive_table_columns + partition_columns
            ]
        )

    def _is_empty_table(self, adb_table_name):
        with closing(self.mysql.get_conn()) as mysql_conn:
            result = mysql_conn.cursor().execute(
                f"select count(*) from {adb_table_name}"
            )
            self.log.info(f"{adb_table_name} rows: {result}")
            return bool(result == 0)

    @staticmethod
    def get_mysql_dataset(**kwargs):
        if (
            "mysql_conn_id" not in kwargs
            or "schema" not in kwargs
            or "sql" not in kwargs
        ):
            raise Exception("Miss parameter mysql_conn_id or metadata or sql.")

        maxrows = 0 if "maxrows" not in kwargs else kwargs["maxrows"]
        how = 1 if "how" not in kwargs else kwargs["how"]

        mysql = MySqlHook(
            mysql_conn_id=kwargs["mysql_conn_id"], schema=kwargs["schema"]
        )
        conn = mysql.get_conn()
        if not conn.open:
            raise Exception("Could not open connection.")
        conn.query(kwargs["sql"])
        result = conn.store_result()
        dataset = result.fetch_row(maxrows=maxrows, how=how)
        conn.close()

        return dataset

    @staticmethod
    def get_hive_sql(catalog, table_name):
        return f"""
        select distinct tb.TBL_NAME as table_name, c.COLUMN_NAME as column_name, c.TYPE_NAME as column_type, c.INTEGER_IDX as seq, c.COMMENT as column_comment
        from hivemeta.DBS db
            inner join hivemeta.TBLS tb on db.DB_ID = tb.DB_ID
            inner join hivemeta.SDS sd on tb.SD_ID = sd.SD_ID
            inner join hivemeta.COLUMNS_V2 c on sd.CD_ID = c.CD_ID
        where db.NAME = '{catalog}'
          and tb.TBL_NAME = '{table_name}'
        order by c.INTEGER_IDX;
        """

    @staticmethod
    def get_hive_partition(catalog, table_name):
        return f"""
        select distinct tb.TBL_NAME as table_name, c.PKEY_NAME as column_name, c.PKEY_TYPE as column_type, c.INTEGER_IDX as seq, c.PKEY_COMMENT as column_comment
        from hivemeta.DBS db
            inner join hivemeta.TBLS tb on db.DB_ID = tb.DB_ID
            inner join hivemeta.PARTITION_KEYS c on tb.TBL_ID = c.TBL_ID
        where db.NAME = '{catalog}'
          and tb.TBL_NAME = '{table_name}'
        order by c.INTEGER_IDX;
        """

    @staticmethod
    def convert_column_type(column_type):
        if column_type == "string":
            return "varchar"
        elif column_type.startswith("array") or column_type.startswith("<<"):
            return "json"
        else:
            return column_type


def data_frame_to_mysql(df, table):
    engine = create_engine(f"mysql+pymysql://zaihui:0P9Tpsho@am-uf60bjp7m8102kdyn131910.ads.aliyuncs.com/dm")
    df.to_sql(con=engine, name=table, if_exists='append', index=False)


def download(file, local_name):
    """resume download orc file from oss"""
    logger.info(f'start download oss file {file}....')
    try:
        oss2.resumable_download(oss_bucket, file, local_name, num_threads=3)
    except Exception as e:
        logger.error(e)
        download(oss_bucket, local_name)


def pre_sync(sync):
    """sync start, create table"""
    if sync.unfinished():
        logger.info('task need not run!')
        raise AirflowSkipException
    sync_task = sync.sync_task
    adb_table = sync_task["adb_table"]
    hive_table = sync_task["hive_table"]
    if sync.dml_operator == 'INSERT INTO':
        # create tmp table
        tmp_mysql_table = f'{sync.adb_table}_tmp'
        create_sql = sync.generate_create_sql(hive_table, tmp_mysql_table)
        sync.drop_adb_table(tmp_mysql_table)
        sync.create_adb_table(tmp_mysql_table, create_sql)
    elif sync.dml_operator == 'REPLACE INTO':
        if not sync.is_adb_table_exists(adb_table):
            create_sql = sync.generate_create_sql(hive_table, adb_table)
            sync.create_adb_table(adb_table, create_sql)


def post_sync(sync):
    """sync finish，rename table"""
    tmp_mysql_table = f'{sync.adb_table}_tmp'
    if sync.dml_operator == "INSERT INTO" and tmp_mysql_table != sync.adb_table \
            and not sync._is_empty_table(tmp_mysql_table):
        # rename tmp table
        sync.drop_adb_table(f"{sync.adb_table}_backup")
        if sync.is_adb_table_exists(sync.adb_table):
            sync.rename_adb_table(sync.adb_table, f"{sync.adb_table}_backup")
        sync.rename_adb_table(tmp_mysql_table, sync.adb_table)


def run_task(sync, files):
    """sync main flow"""
    if isinstance(files, str):
        files = [files]

    for file in files:
        sync_task = sync.sync_task
        adb_table = sync_task["adb_table"]
        tmp_mysql_table = adb_table
        if sync.dml_operator == 'INSERT INTO':
            tmp_mysql_table = f'{adb_table}_tmp'

        file_parts = file.split('/')
        suffix = file_parts[-1]
        local_name = f'{file_addr}/{suffix}'
        start = time.time()
        download(file, local_name)
        logger.info('oss read done')
        with open(local_name, 'rb') as f:
            data = orc.ORCFile(f)
            table = data.read()
            df = table.to_pandas()
            special_columns = sync_task.get('special_columns')
            if special_columns:
                sp = [c for c in special_columns if c in df.columns]
                if sp:
                    df = df.drop(columns=sp)
            num_columns = table.num_columns
            logger.info(table.num_rows)
            logger.info(num_columns)

        # find partitions
        for path in file_parts:
            if '=' in path:
                df[path.split('=')[0]] = path.split('=')[1]

        total = df.shape[0]
        logger.info(total)
        base = 10000
        rn = total // base
        all_tasks = []
        with ThreadPoolExecutor(5 * multiprocessing.cpu_count()) as executor:
            for i in range(rn):
                task = executor.submit(data_frame_to_mysql, df[i*base:(i+1)*base], tmp_mysql_table)
                all_tasks.append(task)
            task = executor.submit(data_frame_to_mysql, df[rn*base:total], tmp_mysql_table)
            all_tasks.append(task)
        wait(all_tasks, return_when=ALL_COMPLETED)

        os.remove(local_name)
        logger.info('sync done!')
        logger.info(time.time()-start)


def sync_operator(batch, dag, sync_task, index, memory_base=None, task_prefix=None):
    """generate operator """
    res = []
    if not task_prefix:
        if dag.dag_id == 'uni_sync':
            task_prefix = sync_task['adb_table']
        else:
            task_prefix = 'batch'
    task_id = f'{task_prefix}_{index}'
    sync = Sync(sync_task, task_id)

    size_base = max([f.size for f in batch]) // 1000000
    memory_base = memory_base if memory_base else int(Variable.get('memory_base'))
    cpu_base = int(Variable.get('cpu_base', default_var=1))
    memory = size_base * memory_base
    memory = max(memory, 2000)
    logger.info(f'{dag.dag_id} {task_id} use memory {memory}')
    executor_config = {
        "KubernetesExecutor":
            {
                "request_cpu": cpu_base,
                "request_memory": f"{memory}Mi"
            }
    }
    res.append(PythonOperator(
        task_id=task_id,
        python_callable=run_task,
        op_kwargs={'sync': sync, 'files': [f.key for f in batch]},
        dag=dag,
        executor_config=executor_config
    ))
    return res


def start_generator(task_id, dag, sync):
    """generate start task"""
    return PythonOperator(
        task_id=task_id,
        dag=dag,
        python_callable=pre_sync,
        op_kwargs={'sync': sync}
    )


def finish_generator(task_id, dag, sync):
    """generate finish task"""
    return PythonOperator(
        task_id=task_id,
        dag=dag,
        python_callable=post_sync,
        op_kwargs={'sync': sync}
    )


def task_generator(dag, sync_task, oss_prefix=None, memory_base=None):
    """the entrance of task generation"""
    result = []
    oss_prefix = oss_prefix if oss_prefix else sync_task['oss_prefix']
    res = oss_bucket.list_objects(oss_prefix)
    index = 0
    acc = 0
    batch = []
    while res.object_list:
        for f in res.object_list:
            if f.size > 0:
                s = f.size / 1000000
                acc += s
                batch.append(f)
                if acc > 50:
                    index += 1
                    result.extend(sync_operator(batch, dag, sync_task, index, memory_base=memory_base))
                    acc = 0
                    batch = []

        if res.next_marker:
            res = oss_bucket.list_objects(oss_prefix, marker=res.next_marker)
        else:
            break
    index += 1
    result.extend(sync_operator(batch, dag, sync_task, index, memory_base=memory_base))
    return result
