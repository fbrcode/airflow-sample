from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


def download_tasks():
    with TaskGroup("downloads", tooltip="Download tasks") as group:
        _ = BashOperator(
            task_id="download_a",
            bash_command="sleep 10",
        )

        _ = BashOperator(
            task_id="download_b",
            bash_command="sleep 10",
        )

        _ = BashOperator(
            task_id="download_c",
            bash_command="sleep 10",
        )

    return group
