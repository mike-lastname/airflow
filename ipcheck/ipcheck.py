from datetime import datetime
import requests

r = requests.get('https://api.ipify.org')
date = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

with open("iplog.txt", "w") as f:
    f.write(f"{date} {r.text}\n")
