from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging

# Redshift Connection
def get_Redshift_connection(autocommit = True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_country_info(url):
    # requests
    data = requests.get(url)
    records = []
    for row in data.json():
        records.append([row["name"]["official"], row["population"], row["area"]])
    return records

@task
def load(schema, table, records):
    # load start
    logging.info("load started")

    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
        CREATE TABLE {schema}.{table}(
            country varchar(150),
            population bigint,
            area varchar(150)
        )
        ;""")
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s::bigint, %s);"
            print(sql, (r[0], r[1], r[2]))
            cur.execute(sql, (r[0], r[1], r[2]))
        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    
    logging.info("load done")


with DAG(
    dag_id='CountryInfo',
    start_date=datetime(2023, 12, 13),
    schedule='30 6 * * Sat',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:
    url = Variable.get("country_url")
    country_data=get_country_info(url)
    load("areacmzl","country_info",country_data)
