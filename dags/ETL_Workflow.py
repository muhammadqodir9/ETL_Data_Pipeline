from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.sensors.sql import SqlSensor


@dag(
    dag_id='ETL_Workflow',
    schedule = "@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=True,

)

def ETL_Workflow():

    check_database = SqlSensor(
        task_id = "check_database",
        conn_id = "supabase_id",
        sql = "select 1 ",
        poke_interval = 40,
        timeout = 45,
    )

    check_database2 = SqlSensor(
        task_id = "check_database2",
        conn_id = "new_supabase_id",
        sql = "select 1 ",
        poke_interval = 40,
        timeout = 45,
    )


ETL_Workflow()



