import requests
import os

# URL for the index file
url = "https://data-argo.ifremer.fr/argo_bio-profile_index.txt"
file_name = "argo_bio-profile_index.txt"
local_path = os.path.join(".", file_name)

# Download the index file
response = requests.get(url)
if response.status_code == 200:
    with open(local_path, 'wb') as file:
        file.write(response.content)
    print(f"Downloaded: {file_name}")
else:
    print(f"Error downloading {file_name}")