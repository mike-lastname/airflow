from airflow.models import Variable # Импорт переменных из Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
import yadisk
from datetime import datetime as dt
from funcs.utils import download_files
import logging


# Получение параметров из переменных Airflow
token = Variable.get("token")
local_folder = Variable.get("local_folder")
disk_folder = Variable.get("disk_folder")
tz = Variable.get("timezone")


# Настройка логгера для вывода сообщений в лог Airflow
logger = logging.getLogger("airflow.task")


# Инициализация клиента Яндекс.Диска с токеном
client = yadisk.Client(token=token)


with DAG(
        dag_id="sync_dag",
        start_date=dt(2024, 11, 27),
        schedule="*/5 * * * *",
        catchup=False,
        params={"hard_sync": False},
        render_template_as_native_obj=True, # Шаблонные переменные передавать как python-объект (по умолчанию - строкой)
):
    download_files_task = PythonOperator(
        task_id="download_files_task",
        python_callable=download_files,
        op_kwargs={
            "hard_sync": "{{ params.hard_sync }}",
            "client": client,
            "local_folder": local_folder,
            "disk_folder": disk_folder,
            "prev_start_date": "{{ prev_start_date_success }}",  # Время предыдущего успешного запуска
            "logger": logger
        }
    )

    # Установка порядка задач (здесь задача одна, но надо указать)
    download_files_task
