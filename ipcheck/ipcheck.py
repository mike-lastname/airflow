from datetime import datetime

import requests

r = requests.get('https://api.ipify.org')
# print(r.text)
print(datetime.now().strftime("%d.%m.%Y %H:%M:%S"), r.text)
