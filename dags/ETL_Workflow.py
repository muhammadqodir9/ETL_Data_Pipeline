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

    @task

    def extract():

        source_hook = PostgresHook(postgres_conn_id="supabase_id")
        df = source_hook.get_pandas_df(
            "select * from olist_customers_dataset limit 100"
        )

        staging_hook = PostgresHook(postgres_conn_id="new_supabase_id")
        conn = staging_hook.get_conn()
        try:

            with conn.cursor() as cursor:

                cursor.execute(
                    """
                    TRUNCATE TABLE extracted_olist_customers_dataset
                    """
                )

                cursor.executemany(
                    """
                    INSERT INTO extracted_olist_customers_dataset (
                        customer_id,
                        customer_unique_id,
                        customer_zip_code_prefix,
                        customer_state,
                        customer_city
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    df.itertuples(
                        index=False,
                        name=None
                    )
                )

            conn.commit()

        except Exception:

            conn.rollback()
            raise

        finally:

            conn.close()

        return "extracted_olist_customers_dataset"


ETL_Workflow()



