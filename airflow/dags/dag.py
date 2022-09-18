from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime

with DAG(
    dag_id="working_pipeline",
    description="parsing_news",
    start_date=datetime(2022,9,16),
    schedule_interval=None
    ) as dag:

    task1 = PapermillOperator(
            task_id="first_task",
            input_nb="/mnt/jobs/parse_news.ipynb",
            output_nb="/mnt/tmp_notebooks/parse_news_output.ipynb"
            )

    task2 = PapermillOperator(
            task_id='second_task',
            input_nb="/mnt/jobs/change_json_to_parq.ipynb",
            output_nb="/mnt/tmp_notebooks/change_json_to_parq_output.ipynb"
            )

    task3 = PapermillOperator(
            task_id='third_task',
            input_nb="/mnt/jobs/write_to_postgre.ipynb",
            output_nb="/mnt/tmp_notebooks/write_to_postgre_output.ipynb"
            )       

    task2.set_upstream(task1)
    task3.set_upstream(task2)