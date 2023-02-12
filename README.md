# Scrape Data from KiteConnect

## Login modified to work with TOTP 2FA

## Usage

- Modify the 'credentials.json' file with your Account Login Credentials and TOTP Secret Key.
- Modify the 'key.json' file with your API Key and API Secret.
- Modify the directories in 'tickdump.py' file and save it
- Run the 'tickdump.py' file to start scraping data

## Requirements

- Selenium
- KiteConnect
- PrettyTable
- Pandas
- furl
- PyOTP
- nsetools
- bs4
