from datetime import datetime
import requests

r = requests.get('https://api.ipify.org').text
date = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

with open("iplog.txt", "a") as f:
    f.write(f"{date} {r}\n")
