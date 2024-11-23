from airflow.decorators import dag, task
from datetime import datetime as dt
import requests


@dag(schedule="@hourly", start_date=dt(2024, 11, 19), catchup=False)
def ip_check():
    @task
    def get_ip():
        r = requests.get('https://api.ipify.org').text
        return r

    @task
    def write_in_file(r):
        date = dt.now().strftime("%d.%m.%Y_%H:%M:%S")
        with open("/home/bind/ip_log.txt", "a") as f:
            f.write(f"{date}: {r}\n")

    write_in_file(get_ip())


ip_check()
