from datetime import datetime
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task

my_file1 = Dataset("/tmp/my_file1.txt")
my_file2 = Dataset("/tmp/my_file2.txt")

with DAG(
    dag_id="consumer",
    start_date=datetime(2023, 1, 1),
    schedule=[my_file1, my_file2],
    tags=["dataset", "consumer"],
    catchup=False,
) as dag:

    @task
    def read_dataset1():
        with open(my_file1.uri, "r") as f:
            print(f.read())

    @task
    def read_dataset2():
        with open(my_file2.uri, "r") as f:
            print(f.read())

    read_dataset1()
    read_dataset2()
