from airflow.decorators import dag, task
from datetime import datetime as dt
from playwright.sync_api import sync_playwright


# DAG, который раз в час проверяет внешний IP и сохраняет его в файл
@dag(schedule="@hourly", start_date=dt(2024, 11, 19), catchup=False)
def ip_check():
    @task
    def get_ip():
        # Запуск Playwright в синхронном режиме
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.goto('https://myip.ru')
            # Извлечение текста из второй строки таблицы на странице (IP-адрес и доп. данные)
            container = page.locator('//table/tbody/tr').nth(1).inner_text()
        return container

    @task
    def write_in_file(container):
        # Формируем временную метку для записи в лог
        date = dt.now().strftime("%d.%m.%Y_%H:%M:%S")
        # Добавляем запись в файл лога с IP и временем проверки
        with open("/home/bind/ip_log.txt", "a") as f:
            f.write(f"{date}: {container}\n")

    # Последовательность выполнения: сначала получаем IP, затем записываем в файл
    write_in_file(get_ip())

# Инициализация DAG
ip_check()
