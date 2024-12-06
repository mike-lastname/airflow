import random
import string
from os import listdir
import yadisk
from airflow.models import Variable
import pathlib
from datetime import datetime as dt


token = Variable.get("token")
client = yadisk.Client(token=token)
local_folder = Variable.get("local_folder")
disk_folder = Variable.get("disk_folder")


def random_string_generator(length):
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def generator():
    file_cnt = random.randint(1, 50)
    res_dict = dict()
    for i in range(file_cnt):
        filename_len = random.randint(5, 10)
        filename = random_string_generator(filename_len)
        data_len = random.randint(100, 200)
        data = random_string_generator(data_len)
        res_dict["filename"] = filename
        res_dict["data"] = data
        yield res_dict


def get_local_folders():
    return set(listdir(local_folder))


def get_disk_folders():
    with client:
        dirs = [i["name"] for i in list(client.listdir(disk_folder, fields=["name"]))]
    return set(dirs)


def check_file(path):
    fname = pathlib.Path(f'{local_folder}/{path.split("/")[2]}/{path.split("/")[3]}')
    if fname.exists():
        return fname.stat().st_mtime
    return float(0)


def get_files_to_update():
    files_to_update = []
    with client:
        r = list(client.get_files(fields=["modified", "path"]))
        for i in r:
            if dt.timestamp(i["modified"]) > check_file(i["path"]):
                files_to_update.append(i["path"])
    return files_to_update
