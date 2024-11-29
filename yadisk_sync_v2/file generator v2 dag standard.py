import yadisk
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
import io
import random
import string
from datetime import datetime as dt
from datetime import timedelta


def random_string_generator(length):
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


with DAG(
        dag_id="file_generator_dag",
        start_date=dt(2024, 11, 27),
        schedule="0 * * * *",
        catchup=False,
        render_template_as_native_obj=True
):
    def generator_uploader():
        token = Variable.get("token")
        client = yadisk.Client(token=token)
        file_cnt = random.randint(1, 50)
        dir_name = dt.now().strftime('%Y%m%d_%H%M%S')
        upload_folder = "/files"
        log = []
        d = dict()
        with client:
            client.mkdir(f"{upload_folder}/{dir_name}")
            for i in range(file_cnt):
                filename_len = random.randint(5, 10)
                data_len = random.randint(100, 200)
                filename = random_string_generator(filename_len)
                data = random_string_generator(data_len).encode()
                data_to_file = io.BytesIO(data)
                client.upload(data_to_file, f"{upload_folder}/{dir_name}/{filename}.txt")
                log.append(f"{dir_name}/{filename}.txt")
        d["log"] = log
        d["dir_name"] = dir_name
        return d


    generate_upload = PythonOperator(
        task_id="generate_upload",
        execution_timeout=timedelta(seconds=100),
        python_callable=generator_uploader,
        op_kwargs={
            "dag_run_time": "{{ts}}"
        }
    )

    def logger(ti):
        d = ti.xcom_pull(task_ids="generate_upload")
        log_folder = "/home/bind/yadisk_sync_v2/logs"
        with open(f"{log_folder}/{d['dir_name']}.txt", "a") as f:
            for i in d["log"]:
                f.write(f"{i}\n")


    write_log = PythonOperator(
        task_id="write_log",
        python_callable=logger
    )

    generate_upload >> write_log
