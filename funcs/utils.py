import random
import string
import os
import pathlib
import io
import pendulum
import yadisk


def random_string_generator(length: int):
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


def get_files_to_dl(client: yadisk.Client, prev_start_date: pendulum.DateTime, hard_sync: bool):
    with client:
        if prev_start_date is None or hard_sync:
            return [i["path"] for i in client.get_files(fields="path")]
        else:
            files_to_dl = []
            for i in client.get_files(fields=["modified", "path"]):
                if (i["modified"]) > prev_start_date:
                    files_to_dl.append(i["path"])
            return files_to_dl


def uploader(task_run_time: str, tz: str, client: yadisk.Client, disk_folder: str, logger):
    local_tz = pendulum.timezone(tz)
    directory_name = local_tz.convert(pendulum.parse(task_run_time)).strftime("%Y%m%d_%H%M%S")
    n_files = 0
    with client:
        if client.exists(f"{disk_folder}/{directory_name}") is False:
            client.mkdir(f"{disk_folder}/{directory_name}")
            logger.info(f"Folder created: {directory_name}")
        for i in generator():
            data_to_file = io.BytesIO((i["data"]).encode())
            client.upload(data_to_file, f"{disk_folder}/{directory_name}/{i['filename']}.txt")
            n_files += 1
    logger.info(f"Files uploaded: {n_files}")
    logger.info(disk_space_info(client))

def random_edit(client: yadisk.Client):
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


def delete_all(local_folder: str):
    os.system(f"rm -rf {local_folder}/*")


def download_files(client: yadisk.Client, local_folder: str, disk_folder: str, prev_start_date: pendulum.DateTime, logger, hard_sync: bool):
    if hard_sync is True:
        logger.info(f"Hard_sync is ON! Deleting all files in '{local_folder}' "
                    f"and downloading all files from 'disk:/{disk_folder}'")
        delete_all(local_folder)
    else:
        logger.info(f"Hard_sync is OFF! Downloading new and edited files from 'disk:/{disk_folder}'")
    files_to_dl = get_files_to_dl(client, prev_start_date, hard_sync)
    for i in files_to_dl:
        if pathlib.Path(local_folder, pathlib.Path(i).parent.name).is_dir() is False:
            pathlib.Path(local_folder, pathlib.Path(i).parent.name).mkdir()
        client.download(f"{i}", f"{pathlib.Path(local_folder, pathlib.Path(i).parent.name, pathlib.Path(i).name)}", overwrite=True)


def size_convert(size_in_bytes: int):
    sizes = {"KB": 1024, "MB": 1024 * 1024, "GB": 1024 * 1024 * 1024}
    for k, v in sizes.items():
        if size_in_bytes / v < 1024:
            return str(f"{round(size_in_bytes / v, 3)} {k}")


def disk_space_info(client: yadisk.Client):
    with client:
        disk_info = client.get_disk_info(fields=["total_space", "used_space"])
        total_space = int(disk_info["total_space"])
        used_space = int(disk_info["used_space"])
        free_space = total_space - used_space
        used_space_conv = size_convert(used_space)
        free_space_conv = size_convert(free_space)
        return print(f"Disk used space: {used_space_conv}. Disk free space: {free_space_conv}.")