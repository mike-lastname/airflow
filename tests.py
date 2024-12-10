# from playwright.sync_api import sync_playwright
#
#
# with sync_playwright() as p:
#     browser = p.chromium.launch()
#     page = browser.new_page()
#     page.goto('https://myip.ru')
#     # container = page.get_by_role('row').all()
#     container = page.locator('//table/tbody/tr').nth(1).inner_text()
#     print(container)

# import yadisk
# # from airflow.models import Variable
# import io
# import random
# import string
# from datetime import datetime as dt
#
#
# TOKEN = "y0_AgAAAAANVHGWAAzRIwAAAAEZWNAsAAA1OYXo9-lM9bMiP3XgkrDjEIxzEw"
# headers = {'Authorization': f'OAuth {TOKEN}'}
#
#
# def generator(length):
#     chars = string.ascii_letters + string.digits
#     return ''.join(random.choice(chars) for _ in range(length))
#
#
# client = yadisk.Client(token=TOKEN)
# file_cnt = random.randint(1, 50)
# dir_name = dt.now().replace(minute=0, second=0, microsecond=0)
# with client:
#     with open("c:/Users/resti/repo/bind/log.txt", "a") as f:
#         client.mkdir(f"/files/{dir_name}")
#         for i in range(file_cnt):
#             filename_len = random.randint(5, 10)
#             data_len = random.randint(100, 1000)
#             filename = generator(filename_len)
#             data = generator(data_len).encode()
#             data_to_file = io.BytesIO(data)
#             client.upload(data_to_file, f"/files/{dir_name}/{filename}.txt")
#             f.write(f"{dir_name}/{filename}.txt\n")

import yadisk
# from airflow.models import Variable
import os
import io
import random
import string
from datetime import datetime as dt
import pathlib


TOKEN = "y0_AgAAAAANVHGWAAzRIwAAAAEZWNAsAAA1OYXo9-lM9bMiP3XgkrDjEIxzEw"


# client = yadisk.Client(token=TOKEN)
# with client:
#     dirs = [i["name"] for i in list(client.listdir("/files", fields=["name", "path", "modified"]))]
#     print(set(dirs))
    # struct_dict = {}
    # dirs = list(client.listdir("/files", fields=["name"]))
    # for i in dirs:
    #     pathlib.Path(f"C:/Users/resti/repo/bind/yadisk_sync_v2/files/{i['name']}").mkdir(exist_ok=True)
    #     for n in client.listdir(f"/files/{i['name']}", fields=["name", "modified", "path"]):
    #         client.download(f"{n['path']}" ,f"C:/Users/resti/repo/bind/yadisk_sync_v2/files/{i['name']}/{n['name']}")
# home = "C:/Users/resti/repo/bind/yadisk_sync_v2/files"
# print(os.listdir(home))

