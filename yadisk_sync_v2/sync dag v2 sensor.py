from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import yadisk
import os
from datetime import datetime as dt
import pathlib


with DAG(
        dag_id="yadisk_sync_dag",
        start_date=dt(2024, 11, 27),
        schedule=None,
        catchup=False,
        render_template_as_native_obj=True
):
    waiting_for_log = FileSensor(
        task_id="waiting_for_log",
        fs_conn_id="fs_conn_1",
        filepath="log.txt",
        poke_interval=5
    )

    def get_local_folders():
        local_folder = "/home/bind/yadisk_sync_v2/files/"
        return set(os.listdir(local_folder))


    def get_disk_folders():
        disk_folder = "/files"
        token = Variable.get("token")
        client = yadisk.Client(token=token)
        with client:
            dirs = [i["name"] for i in list(client.listdir(disk_folder, fields=["name"]))]
        return set(dirs)


    get_local_folders_task = PythonOperator(
        task_id="get_local_folders_task",
        # execution_timeout=timedelta(seconds=100),
        python_callable=get_local_folders
    )

    get_disk_folders_task = PythonOperator(
        task_id="get_disk_folders_task",
        python_callable=get_disk_folders
    )


    def get_folders_to_dl(ti):
        local_folders = ti.xcom_pull(task_ids="get_local_folders_task")
        disk_folders = ti.xcom_pull(task_ids="get_disk_folders_task")
        folders_to_dl = disk_folders - local_folders
        return list(folders_to_dl)


    get_folders_to_dl_task = PythonOperator(
        task_id="get_folders_to_dl_task",
        python_callable=get_folders_to_dl
    )


    def download_files(ti):
        folders_to_dl = ti.xcom_pull(task_ids="get_folders_to_dl_task")
        local_folder = "/home/bind/yadisk_sync_v2/files/"
        disk_folder = "/files"
        token = Variable.get("token")
        client = yadisk.Client(token=token)
        with client:
            for i in folders_to_dl:
                pathlib.Path(f"{local_folder}/{i}").mkdir(exist_ok=True)
                for n in client.listdir(f"{disk_folder}/{i}", fields=["name", "modified", "path"]):
                    client.download(f"{n['path']}", f"{local_folder}/{i}/{n['name']}")

    download_files_task = PythonOperator(
        task_id="download_files_task",
        python_callable=download_files
    )

    waiting_for_log >> [get_disk_folders_task, get_local_folders_task] >> get_folders_to_dl_task >> download_files_task
