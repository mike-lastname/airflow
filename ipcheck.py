from airflow.decorators import dag, task
from datetime import datetime as dt
from playwright.sync_api import sync_playwright

@dag(schedule="@hourly", start_date=dt(2024, 11, 19), catchup=False)
def ip_check():
    @task
    def get_ip():
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.goto('https://myip.ru')
            container = page.locator('//table/tbody/tr').nth(1).inner_text()
        return container

    @task
    def write_in_file(container):
        date = dt.now().strftime("%d.%m.%Y_%H:%M:%S")
        with open("/home/bind/ip_log.txt", "a") as f:
            f.write(f"{date}: {container}\n")

    write_in_file(get_ip())

ip_check()
