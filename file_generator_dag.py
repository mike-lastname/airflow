from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
import yadisk
from funcs.utils import uploader, random_edit
import logging


token = Variable.get("token")
disk_folder = Variable.get("disk_folder")
tz = Variable.get("timezone")


logger = logging.getLogger("airflow.task")


client = yadisk.Client(token=token)


with DAG(
        dag_id="file_generator_dag",
        start_date=pendulum.datetime(2024, 11, 27),
        schedule="0 * * * *",
        catchup=False,
):
    upload = PythonOperator(
        task_id="upload",
        python_callable=uploader,
        op_kwargs={
            "task_run_time": "{{ts_nodash_with_tz}}",
            "tz": tz,
            "client": client,
            "disk_folder": disk_folder,
            "logger": logger
        }
    )

    edit = PythonOperator(
        task_id="edit",
        python_callable=random_edit,
        op_kwargs={
            "client": client
        }
    )

    upload >> edit
