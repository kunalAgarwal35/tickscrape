# Imports

import os
import requests
import subprocess
import re
import traceback
import xml.etree.ElementTree as ET
import zipfile
import io
import sys
from bs4 import BeautifulSoup
import shutil
# Constants

digits_re = re.compile(r'\d+')

# Functions

def get_chrome_version() -> str:
    """Get chrome version from command line"""
    digits_re = re.compile('\d+')
    command = 'reg query "HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon" /v version'
    output = subprocess.check_output(command, shell=True)
    try:
        version = ".".join(digits_re.findall(output.decode("utf8").split("\n")[2].strip()))
        return version
    except (IndexError, ValueError, TypeError) as e:
        print(traceback.format_exc())
        return ''

def get_chromedriver_version() -> str:
    """Get chromedriver version from command line"""
    command = 'chromedriver --version'
    output = subprocess.check_output(command, shell=True)
    try:
        version = output.decode("utf8").split(" ")[1].strip()
        return version
    except (IndexError, ValueError, TypeError) as e:
        print(traceback.format_exc())
        return ''

def extract_version_from_url_keep_decimals(url: str) -> str:
    """
    Extracts and returns the version number from a given URL as a string, keeping the decimal points.

    Parameters:
    - url (str): The URL string containing the version number.

    Returns:
    - str: The extracted version number as a string.
    """
    # Regular expression to match the version number pattern
    version_pattern = re.compile(r'/(\d+\.\d+\.\d+\.\d+)/')

    # Search for the version number pattern in the URL
    match = version_pattern.search(url)
    if match:
        # Extract the version number string
        return match.group(1)
    else:
        # If no version number found, return an empty string or an error indicator
        return "Version not found"

def get_chromedriver_link(version: str, version_number: str, platform: str, arch: str) -> str:
    base_url = "https://googlechromelabs.github.io/chrome-for-testing/#stable"
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+\.zip')
    urls = re.findall(url_pattern, str(soup))

    fname = 'chromedriver-' + platform + arch + '.zip'

    # Iterate through the rows to find the correct download link
    for url in urls:
        if fname in url and version_number in url:
            print(url)
            return url
    from distutils.version import LooseVersion
    available_versions = [extract_version_from_url_keep_decimals(url) for url in urls if fname in url]
    current_version_lv = LooseVersion(version_number)

    closest_version = None
    min_distance = None

    for version in available_versions:
        version_lv = LooseVersion(version)
        # Calculate the "distance" based on comparison, not subtraction
        distance = (version_lv > current_version_lv) - (version_lv < current_version_lv)

        if closest_version is None or (0 <= distance < min_distance):
            closest_version = version
            min_distance = distance
    print('Downloading the closest version:', closest_version)
    for url in urls:
        if fname in url and closest_version in url:
            print(url)
            return url

    raise Exception("Could not get the download link.")


# def get_chromedriver_link(version: str, system: str, arch: str) -> str:
#     """Get link of chromedriver from website"""
#     links = list()
#     link = 'https://chromedriver.storage.googleapis.com/'
#     root = ET.fromstring(
#         requests.get(link).content.decode('utf8')
#     )
#     for child in root:
#         for subchild in child:
#             if subchild.text.startswith(version):
#                 if system in subchild.text:
#                     if arch in subchild.text:
#                         links.append(link+subchild.text) # link + subchild.text
#     if len(links) == 0:
#         return ''
#     else:
#         return links[-1]

def download_and_save_chromedriver(download_link: str, output_path: str) -> str:
    """Download chromedriver and save to given directory. Returns the path of chromedriver"""
    resp = requests.get(download_link)
    # Check if the request was successful
    if resp.status_code != 200:
        print(f"Failed to download from {download_link}. Status code: {resp.status_code}")
        return ''

    # Check if the content is a ZIP file
    # if resp.headers.get("content-type") != 'application/zip':
    #     print(f"Unexpected content type: {resp.headers.get('content-type')}")
    #     return ''

    # Ensure directory exists
    if not os.path.isdir(output_path):
        os.makedirs(output_path)

    # Extract the ZIP content to a temporary folder
    temp_folder = os.path.join(output_path, "temp_chromedriver_folder")
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zip_file:
        zip_file.extractall(path=temp_folder)

    # Find the subdirectory inside the temporary folder
    subdirs = [d for d in os.listdir(temp_folder) if os.path.isdir(os.path.join(temp_folder, d))]
    if not subdirs:
        print("No subdirectory found inside the ZIP content.")
        return ''
    inner_folder = os.path.join(temp_folder, subdirs[0])

    # Move chromedriver and license file to the desired output path
    chromedriver_path_src = os.path.join(inner_folder, 'chromedriver.exe')
    # license_path_src = os.path.join(inner_folder, 'LICENSE')

    chromedriver_path_dest = os.path.join(output_path, 'chromedriver.exe')
    # license_path_dest = os.path.join(output_path, 'LICENSE')

    shutil.move(chromedriver_path_src, chromedriver_path_dest)
    # shutil.move(license_path_src, license_path_dest)

    # Remove the temporary folder
    shutil.rmtree(temp_folder)

    return chromedriver_path_dest

def get_platform_architecture():
    """Get OS and CPU info"""
    if sys.platform.startswith('linux') and sys.maxsize > 2 ** 32:
        platform = 'linux'
        architecture = '64'
    elif sys.platform == 'darwin':
        platform = 'mac'
        architecture = '64'
    elif sys.platform.startswith('win'):
        platform = 'win'
        architecture = '32'
    else:
        raise RuntimeError('Could not determine chromedriver download URL for this platform.')
    return platform, architecture


def main(output_path = ".", force=False) -> bool:
    """Fetch and save current compatible version of chromedriver from google's website"""
    version = get_chrome_version()
    print("Chrome Version:", version)
    major_version = ".".join(
        version.split(".")[:2]
    )
    try:
        chromedriver_version = get_chromedriver_version()
        print("Chromedriver Version:", chromedriver_version)
        major_version_chromedriver = ".".join(
            chromedriver_version.split(".")[:2]
        )
        if major_version == major_version_chromedriver and not force:
            print("Chromedriver is up to date.")
            return 0
    except Exception as e:
        chromedriver_version = None

    platform, arch = get_platform_architecture()
    link = get_chromedriver_link("Stable", version, platform, arch)
    print("Downloading from:", link)
    if not link:
        raise Exception("Could not get the download link.")
    abspath = os.path.abspath(output_path)
    savepath = download_and_save_chromedriver(link, abspath)
    if savepath:
        print("Saved to:", savepath)
        return 0
    return 1


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', type=str, default='.', help='Output directory')
    parser.add_argument('-f', '--force', action='store_true', default=False, help='Force update chromedriver')
    args = parser.parse_args()
    sys.exit(main(args.output, args.force))
    import math