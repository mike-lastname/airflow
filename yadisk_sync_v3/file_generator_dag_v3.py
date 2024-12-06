from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import io
import random
import pendulum
import yadisk
from funcs.utils import generator


token = Variable.get("token")
disk_folder = Variable.get("disk_folder")
tz = Variable.get("timezone")
client = yadisk.Client(token=token)


def uploader(task_run_time: str):
    local_tz = pendulum.timezone(tz)
    local_time_str = local_tz.convert(pendulum.parse(task_run_time)).strftime("%Y%m%d_%H%M%S")
    directory_name = local_time_str
    with client:
        client.mkdir(f"{disk_folder}/{directory_name}")
        for i in generator():
            data_to_file = io.BytesIO((i["data"]).encode())
            client.upload(data_to_file, f"{disk_folder}/{directory_name}/{i['filename']}.txt")


def random_edit():
    with client:
        all_files = [i["path"] for i in client.get_files(fields="path")]
        edit_files = []
        for i in range(5):
            edit_files.append(all_files[random.randint(0, len(all_files) - 1)])
        for i in edit_files:
            dl = io.BytesIO()
            client.download(i, dl)
            dl.write(b"random_string")
            dl.seek(0)
            client.upload(dl, i, overwrite=True)


with DAG(
        dag_id="file_generator_dag_v3",
        start_date=pendulum.datetime(2024, 11, 27),
        schedule="0 * * * *",
        catchup=False,
):
    upload = PythonOperator(
        task_id="upload",
        python_callable=uploader,
        op_kwargs={
            "task_run_time": "{{ts_nodash_with_tz}}"
        }
    )

    edit = PythonOperator(
        task_id="edit",
        python_callable=random_edit,
    )

    upload >> edit
