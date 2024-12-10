from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
import yadisk
from datetime import datetime as dt
from funcs.utils import download_files
import logging


token = Variable.get("token")
local_folder = Variable.get("local_folder")
disk_folder = Variable.get("disk_folder")
tz = Variable.get("timezone")


logger = logging.getLogger("airflow.task")


client = yadisk.Client(token=token)


with DAG(
        dag_id="sync_dag",
        start_date=dt(2024, 11, 27),
        schedule="*/5 * * * *",
        catchup=False,
        params={"hard_sync": False},
        render_template_as_native_obj=True,
):
    download_files_task = PythonOperator(
        task_id="download_files_task",
        python_callable=download_files,
        op_kwargs={
            "hard_sync": "{{ params.hard_sync }}",
            "client": client,
            "local_folder": local_folder,
            "disk_folder": disk_folder,
            "prev_start_date": "{{ prev_start_date_success }}",
            "logger": logger
        }
    )

    # update_files_task = PythonOperator(
    #     task_id="update_files_task",
    #     python_callable=update_files,
    #     op_kwargs={
    #         "ts": "{{ ts }}",
    #         "client": client,
    #         "local_folder": local_folder
    #     }
    # )

    download_files_task
