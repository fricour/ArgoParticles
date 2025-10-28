import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin

# List of WMO numbers
#WMO = [6903093, 6903094, 3902498, 6904241, 2903783, 1902593, 4903657, 5906970, 4903634, 1902578, 3902471, 4903658, 6990503, 2903787, 4903660, 6990514, 1902601, 4903739, 1902637, 4903740, 2903794, 1902685, 6904240, 7901028]
WMO = [1902578, 1902593, 1902601, 1902637, 1902685, 2903783, 2903787, 2903794, 3902471, 3902498, 4903634, 4903657, 4903658, 4903660, 4903739, 4903740, 5906970, 6904240, 6904241, 6990503, 6990514, 7901028]

DOWNLOAD_DIR = "argo_profiles"

def get_file_list(wmo):
    BASE_URL = f"https://data-argo.ifremer.fr/aux/coriolis/{wmo}/profiles/"
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        print(f"Accessing URL for WMO {wmo} OK")
        soup = BeautifulSoup(response.text, 'html.parser')
        print(soup)
        return BASE_URL, [link.get('href') for link in soup.find_all('a') if link.get('href').endswith('.nc')]
    else:
        print(f"Error accessing URL for WMO {wmo}")
        return BASE_URL, []

def download_file(base_url, file_name, wmo):
    file_url = urljoin(base_url, file_name)
    local_dir = os.path.join(DOWNLOAD_DIR, str(wmo))
    local_path = os.path.join(local_dir, file_name)
    
    if os.path.exists(local_path):
        print(f"File already exists: {file_name} for WMO {wmo}")
        return False
    
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    
    response = requests.get(file_url)
    if response.status_code == 200:
        with open(local_path, 'wb') as file:
            print(f"Downloading {file_name} for WMO {wmo}")
            file.write(response.content)
        print(f"Downloaded: {file_name} for WMO {wmo}")
        return True
    else:
        print(f"Error downloading {file_name} for WMO {wmo}")
        return False

def main():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

    total_new_files = 0

    for wmo in WMO:
        base_url, files = get_file_list(str(wmo))
        
        new_files = 0
        for file in files:
            if download_file(base_url, file, wmo):
                new_files += 1
        
        total_new_files += new_files
        print(f"{new_files} new files downloaded for WMO {wmo}.")

    print(f"Download completed. Total of {total_new_files} new files downloaded.")

if __name__ == "__main__":
    main()
