import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin

# List of WMO numbers
WMO = [1902578, 1902593, 1902601, 1902637, 1902685, 2903783, 2903787, 2903794, 3902471, 3902498, 4903634, 4903657, 4903658, 4903660, 4903739, 4903740, 5906970, 6904240, 6904241, 6990503, 6990514, 7901028]

DOWNLOAD_DIR = "argo_core_trajectory_files"

def download_file(wmo):
    base_url = f"https://data-argo.ifremer.fr/dac/coriolis/{wmo}/"
    file_name = str(wmo)+'_Rtraj.nc'
    print("WMO: ", str(wmo))
    file_url = urljoin(base_url, file_name)
    local_dir = os.path.join(DOWNLOAD_DIR, str(wmo))
    local_path = os.path.join(local_dir, file_name)
    
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    
    response = requests.get(file_url)
    if response.status_code == 200:
        with open(local_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {file_name} for WMO {wmo}")
        return True
    else:
        print(f"Error downloading {file_name} for WMO {wmo}")
        return False

def main():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

    for wmo in WMO:
        download_file(wmo)

if __name__ == "__main__":
    main()
