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


def get_chromedriver_link(version: str, system: str, arch: str) -> str:
    """Get link of chromedriver from website"""
    links = list()
    link = 'https://chromedriver.storage.googleapis.com/'
    root = ET.fromstring(
        requests.get(link).content.decode('utf8')
    )
    for child in root:
        for subchild in child:
            if subchild.text.startswith(version):
                if system in subchild.text:
                    if arch in subchild.text:
                        links.append(link+subchild.text) # link + subchild.text
    if len(links) == 0:
        return ''
    else:
        return links[-1]

def download_and_save_chromedriver(download_link: str, output_path: str) -> str:
    """Download chromedriver and save to given directory. Returns the path of chromedriver"""
    resp = requests.get(download_link)
    if resp.headers.get("content-type") == 'application/zip':
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zip_file:
            for zipinfo in zip_file.infolist():
                if not os.path.isdir(output_path):
                    os.makedirs(output_path)
                zip_file.extract(zipinfo.filename, path=output_path)
                if os.path.isfile((out:= os.path.join(output_path, zipinfo.filename))):
                    return out
    return ''


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
    link = get_chromedriver_link(major_version, platform, arch)
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
