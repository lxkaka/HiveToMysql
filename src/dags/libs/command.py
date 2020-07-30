#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pendulum
from airflow.hooks.mysql_hook import MySqlHook

local_tz = pendulum.timezone("Asia/Shanghai")


def get_mysql_dataset(**kwargs):
    if "mysql_conn_id" not in kwargs or "schema" not in kwargs or "sql" not in kwargs:
        raise Exception("Miss parameter mysql_conn_id or metadata or sql.")

    maxrows = 0 if "maxrows" not in kwargs else kwargs["maxrows"]
    how = 1 if "how" not in kwargs else kwargs["how"]

    mysql = MySqlHook(mysql_conn_id=kwargs["mysql_conn_id"], schema=kwargs["schema"])
    conn = mysql.get_conn()
    if not conn.open:
        raise Exception("Could not open connection.")
    conn.query(kwargs["sql"])
    result = conn.store_result()
    dataset = result.fetch_row(maxrows=maxrows, how=how)
    conn.close()

    return dataset
