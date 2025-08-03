from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
import yadisk
from funcs.utils import uploader, random_edit
import logging


# Получение параметров из переменных Airflow
token = Variable.get("token")
disk_folder = Variable.get("disk_folder")
tz = Variable.get("timezone")


# Настройка логгера для вывода сообщений в лог Airflow
logger = logging.getLogger("airflow.task")


# Инициализация клиента Яндекс.Диска с токеном
client = yadisk.Client(token=token)


with DAG(
        dag_id="file_generator_dag",
        start_date=pendulum.datetime(2024, 11, 27),
        schedule="0 * * * *",
        catchup=False,
):
    # Задача загрузки файла на Яндекс.Диск
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

    # Задача случайного редактирования файла на Яндекс.Диске
    edit = PythonOperator(
        task_id="edit",
        python_callable=random_edit,
        op_kwargs={
            "client": client
        }
    )

    # Установка порядка задач
    upload >> edit
