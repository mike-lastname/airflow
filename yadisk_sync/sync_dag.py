from airflow.decorators import dag, task
import yadisk
import os
from datetime import datetime as dt

client = yadisk.Client(token="")


@dag(schedule="@hourly", start_date=dt(2024, 11, 23), catchup=False)
def ya_disk_sync():
    @task
    def get_local_files_list():
        return os.listdir("/home/bind/files/")

    @task
    def get_disk_files_list():
        files_list = []
        for i in list(client.listdir("/files")):
            files_list.append(i["name"])
        return files_list

    @task
    def get_files_to_dl_list(disk_files_list, local_files_list):
        files_to_download = []
        for i in disk_files_list:
            if i not in local_files_list:
                files_to_download.append(i)
        return files_to_download

    @task
    def download_files(files_to_download):
        for i in files_to_download:
            client.download(f"/files/{i}", f"/home/bind/files/{i}")

    download_files(get_files_to_dl_list(get_disk_files_list(), get_local_files_list()))


ya_disk_sync()
