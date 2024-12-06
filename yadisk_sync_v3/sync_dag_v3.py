from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
import yadisk
from datetime import datetime as dt
from pathlib import Path
from funcs.utils import get_disk_folders, get_local_folders, get_files_to_update
import os
import shutil


token = Variable.get("token")
local_folder = Variable.get("local_folder")
disk_folder = Variable.get("disk_folder")
tz = Variable.get("timezone")


client = yadisk.Client(token=token)


def download_files(hard_sync: str):
    if hard_sync == "true":
        for i in os.listdir(local_folder):
            shutil.rmtree(os.path.join(local_folder, i))
        with client:
            for i in client.listdir(disk_folder, fields=["name"]):
                Path(f"{local_folder}/{i['name']}").mkdir()
                for n in client.listdir(f"{disk_folder}/{i['name']}", fields=["name", "path"]):
                    client.download(f"{n['path']}", f"{local_folder}/{i['name']}/{n['name']}", overwrite=True)
    else:
        folders_to_dl = list(get_disk_folders() - get_local_folders())
        with client:
            for i in folders_to_dl:
                Path(f"{local_folder}/{i}").mkdir(exist_ok=True)
                for n in client.listdir(f"{disk_folder}/{i}", fields=["name", "modified", "path"]):
                    client.download(f"{n['path']}", f"{local_folder}/{i}/{n['name']}", overwrite=True)


def update_files():
    files_to_update = get_files_to_update()
    for i in files_to_update:
        client.download(f"{i}", f"{local_folder}/{i.split('/')[2]}/{i.split('/')[3]}")


with DAG(
        dag_id="sync_dag_v3",
        start_date=dt(2024, 11, 27),
        schedule="*/5 * * * *",
        catchup=False,
        params={"hard_sync": "true"}
):
    download_files_task = PythonOperator(
        task_id="download_files_task",
        python_callable=download_files,
        op_kwargs={"hard_sync": "{{ params.hard_sync }}"}
    )

    update_files_task = PythonOperator(
        task_id="update_files_task",
        python_callable=update_files,
    )

    download_files_task >> update_files_task
