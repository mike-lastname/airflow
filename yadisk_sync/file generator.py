import requests
import random_file_generator


TOKEN = 'y0_AgAAAAANVHGWAAzRIwAAAAEZWNAsAAA1OYXo9-lM9bMiP3XgkrDjEIxzEw'
headers = {'Authorization': f'OAuth {TOKEN}'}

filename = random_file_generator.generate_random_text(5)
data = random_file_generator.generate_random_text(100)

upload_url_request = requests.get("https://cloud-api.yandex.net/v1/disk/resources/upload?"f"path=/files/{filename}.txt", headers=headers).json()
upload_url = upload_url_request.get("href")
upload_request = requests.put(upload_url, data=data, headers=headers)

# r = requests.get("https://cloud-api.yandex.net/v1/disk/resources?path=disk:/files", headers=headers).json()
# print(r['_embedded']['items'])

# for key in r['_embedded']['items']:
#     print(key, ":", r['_embedded']['items'][key])

