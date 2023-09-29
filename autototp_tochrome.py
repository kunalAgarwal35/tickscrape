import cv2
import numpy as np
import os
import pandas as pd
from pyotp import TOTP
import json
import pyautogui
import time

def read_credentials():
    with open("credentials.json", "r") as f:
        return json.load(f)

def get_totp():
    credentials = read_credentials()
    totp = TOTP(credentials["totp"])
    return totp.now()


def get_image(path):
    img = cv2.imread(path)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return img


def get_screenshot():
    img = pyautogui.screenshot()
    img = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return img

def templatematch():
    screen = get_screenshot()
    image_filename = 'loginscreen.png'
    template = get_image(image_filename)
    w, h = template.shape[::-1]
    res = cv2.matchTemplate(screen, template, cv2.TM_CCOEFF_NORMED)
    threshold = 0.8
    loc = np.where(res >= threshold)
    # return true if found
    if len(loc[0]) > 0:
        print('found')
        return True
    else:
        print('not found')
        return False



def main():
    while True:
        time.sleep(2)
        if templatematch():
            print('found')
            pyautogui.typewrite(get_totp())
            pyautogui.press('enter')
        else:
            print('not found')

if __name__ == '__main__':
    main()
