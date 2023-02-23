from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from pandas import json_normalize


# ti parameter means task instance
def _process_user(ti):
    """Processes user and converts it into a JSON object."""
    user = ti.xcom_pull(task_ids="extract_user")  # get user from xcom
    user = user["results"][0]  # get first user
    # process_user = json_normalize(user)  # convert to JSON
    process_user = json_normalize(
        {
            "first_name": user["name"]["first"],
            "last_name": user["name"]["last"],
            "country": user["location"]["country"],
            "username": user["login"]["username"],
            "password": user["login"]["password"],
            "email": user["email"],
        }
    )
    process_user.to_csv("/tmp/processed_user.csv", index=None, header=True)
    # return json.dumps(user)  # convert to JSON


def _store_user():
    """Stores user in Postgres."""
    pg_hook = PostgresHook(postgres_conn_id="postgres")
    # pg_hook.run("INSERT INTO app.user VALUES ('test', 'test', 'test', 'test', 'test', 'test')")
    pg_hook.copy_expert(
        "COPY app.user FROM stdin WITH CSV HEADER DELIMITER as ','",
        "/tmp/processed_user.csv",
    )


with DAG(
    dag_id="user_processing",  # name of DAG (identifier)
    start_date=datetime(2023, 1, 1),  # start date
    schedule="@daily",  # triggered every day at midnight
    catchup=False,  # do not run for past dates
) as dag:
    # task definitions (one PythonOperator per Task)
    # 3 types of operators:
    # - Action operators (purely execute): PythonOperator (execute python function), BashOperator (execute a bash command), and SimpleHttpOperator
    # - Transfer operators (in charge of transferring data from A to B): S3ToHiveTransfer, S3ToRedshiftOperator, and S3ToSFTPOperator
    # - Sensor operators (wait for an event): S3KeySensor, S3PrefixSensor, and HdfsSensor

    # [START create_user_table]
    ### Need to create a postgres connection in Airflow UI
    # connection id = postgres
    # connection type = Postgres
    # host = postgres
    # login = airflow
    # password = airflow
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql="""
        CREATE SCHEMA IF NOT EXISTS app;
        CREATE TABLE IF NOT EXISTS app.user (
            email TEXT PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            country TEXT NOT NULL,
            username  TEXT NOT NULL,
            password TEXT NOT NULL
        );
        """,
    )
    # [END create_user_table]

    # [START check_for_api_availability]
    ### Need to create an HTTP connection in Airflow UI
    # connection id = user_api
    # connection type = HTTP
    # host = https://randomuser.me/
    # ###########################
    # API test endpoint:
    #   curl https://randomuser.me/api/ --silent | jq .
    # ###########################
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="user_api",
        endpoint="api/",
    )

    # [END check_for_api_availability]

    # [START extract_user]
    extract_user = SimpleHttpOperator(
        task_id="extract_user",
        http_conn_id="user_api",  # connection to user_api
        endpoint="api/",  # endpoint to get users
        method="GET",
        response_filter=lambda response: json.loads(response.text),  # filter response
        log_response=True,
    )
    # [END extract_user]

    # [START process_user]
    process_user = PythonOperator(
        task_id="process_user",
        python_callable=_process_user,
    )
    # [END process_user]

    # [START store_user]
    store_user = PythonOperator(
        task_id="store_user",
        python_callable=_store_user,
    )
    # [END store_user]

    # [START dag_dependencies]
    create_table >> is_api_available >> extract_user >> process_user >> store_user
    # [END dag_dependencies]

if __name__ == "__main__":
    dag.test()
