from datetime import datetime
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task

my_file1 = Dataset("/tmp/my_file1.txt")
my_file2 = Dataset("/tmp/my_file2.txt")

with DAG(
    dag_id="producer",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    tags=["dataset", "producer"],
    catchup=False,
) as dag:

    @task(outlets=[my_file1])
    def update_dataset1():
        with open(my_file1.uri, "a+") as f:
            f.write("Hey!! Producer updated dataset #1")

    @task(outlets=[my_file2])
    def update_dataset2():
        with open(my_file2.uri, "a+") as f:
            f.write("Hey!! Producer updated dataset #2")

    update_dataset1() >> update_dataset2()
