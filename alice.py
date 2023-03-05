import requests
from alice_blue import *
import pyotp
import hashlib
import hmac
import time
import logging
import tempfile
import os
import json
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from time import sleep
from urllib.parse import urlparse, parse_qs
import base64


# this file will be used to make requests to the aliceblue api
# the purpose will be to get margin details and place orders accordingly


class CryptoJsAES:
    @staticmethod
    def __pad(data):
        BLOCK_SIZE = 16
        length = BLOCK_SIZE - (len(data) % BLOCK_SIZE)
        return data + (chr(length)*length).encode()

    @staticmethod
    def __unpad(data):
        return data[:-(data[-1] if type(data[-1]) == int else ord(data[-1]))]

    @staticmethod
    def __bytes_to_key(data, salt, output=48):
        assert len(salt) == 8, len(salt)
        data += salt
        key = hashlib.md5(data).digest()
        final_key = key
        while len(final_key) < output:
            key = hashlib.md5(key + data).digest()
            final_key += key
        return final_key[:output]

    @staticmethod
    def encrypt(message, passphrase):
        salt = os.urandom(8)
        key_iv = CryptoJsAES.__bytes_to_key(passphrase, salt, 32+16)
        key = key_iv[:32]
        iv = key_iv[32:]
        aes = Cipher(algorithms.AES(key), modes.CBC(iv))
        return base64.b64encode(b"Salted__" + salt + aes.encryptor().update(CryptoJsAES.__pad(message)) + aes.encryptor().finalize())

    @staticmethod
    def decrypt(encrypted, passphrase):
        encrypted = base64.b64decode(encrypted)
        assert encrypted[0:8] == b"Salted__"
        salt = encrypted[8:16]
        key_iv = CryptoJsAES.__bytes_to_key(passphrase, salt, 32+16)
        key = key_iv[:32]
        iv = key_iv[32:]
        aes = Cipher(algorithms.AES(key), modes.CBC(iv))
        return CryptoJsAES.__unpad(aes.decryptor.update(encrypted[16:]) + aes.decryptor().finalize())
class alice():
    def __init__(self, api_key=None, api_secret=None, user_id=None, password=None,twofa=None):
        self.base_url = 'https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api/'
        self.api_key = api_key
        self.api_secret = api_secret
        self.user_id = user_id
        self.password = password
        self.twofa = twofa
        # self.session_id = AliceBlue.login_and_get_sessionID(username=user_id,
        #                                                password=password,
        #                                                twoFA="1995",
        #                                                app_id=api_key,
        #                                                api_secret=api_secret)
        # self.alice_obj = AliceBlue(username=user_id, session_id=self.session_id)
        self.host = "https://a3.aliceblueonline.com/rest/AliceBlueAPIService"
        self.__urls = {"webLogin": f"{self.host}/customer/webLogin",
                  "twoFA": f"{self.host}/sso/validAnswer",
                  "sessionID": f"{self.host}/sso/getUserDetails",
                  "getEncKey": f"{self.host}/customer/getEncryptionKey",
                  "authorizeVendor": f"{self.host}/sso/authorizeVendor",
                  "apiGetEncKey": f"{self.host}/api/customer/getAPIEncpkey",
                  "profile": f"{self.host}/api/customer/accountDetails",
                  "placeOrder": f"{self.host}/api/placeOrder/executePlaceOrder",
                  "logout": f"{self.host}/api/customer/logout",
                  "logoutFromAllDevices": f"{self.host}/api/customer/logOutFromAllDevice",
                  "fetchMWList": f"{self.host}/api/marketWatch/fetchMWList",
                  "fetchMWScrips": f"{self.host}/api/marketWatch/fetchMWScrips",
                  "addScripToMW": f"{self.host}/api/marketWatch/addScripToMW",
                  "deleteMWScrip": f"{self.host}/api/marketWatch/deleteMWScrip",
                  "scripDetails": f"{self.host}/api/ScripDetails/getScripQuoteDetails",
                  "positions": f"{self.host}/api/positionAndHoldings/positionBook",
                  "holdings": f"{self.host}/api/positionAndHoldings/holdings",
                  "sqrOfPosition": f"{self.host}/api/positionAndHoldings/sqrOofPosition",
                  "fetchOrder": f"{self.host}/api/placeOrder/fetchOrderBook",
                  "fetchTrade": f"{self.host}/api/placeOrder/fetchTradeBook",
                  "exitBracketOrder": f"{self.host}/api/placeOrder/exitBracketOrder",
                  "modifyOrder": f"{self.host}/api/placeOrder/modifyOrder",
                  "cancelOrder": f"{self.host}/api/placeOrder/cancelOrder",
                  "orderHistory": f"{self.host}/api/placeOrder/orderHistory",
                  "getRmsLimits": f"{self.host}/api/limits/getRmsLimits",
                  "createWsSession": f"{self.host}/api/ws/createSocketSess",
                  "history": f"{self.host}/api/chart/history",
                  "master_contract": "https://v2api.aliceblueonline.com/restpy/contract_master?exch={exchange}",
                  "ws": "wss://ws2.aliceblueonline.com/NorenWS/"
                  }
        self.session_id = AliceBlue.login_and_get_sessionID(username=user_id,
                                                         password=password,
                                                            twoFA="1995",
                                                            app_id=api_key,
                                                            api_secret=api_secret)
        self.alice_obj = AliceBlue(username=user_id, session_id=self.session_id)


if __name__ == '__main__':
    __urls = {"webLogin": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/customer/webLogin",

              "twoFA": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/sso/validAnswer",
              "sessionID": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/sso/getUserDetails",
              "getEncKey": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/customer/getEncryptionKey",
              "authorizeVendor": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/sso/authorizeVendor",
              "apiGetEncKey": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/customer/getAPIEncpkey",
              "profile": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/customer/accountDetails",
              "placeOrder": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/placeOrder/executePlaceOrder",
              "logout": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/customer/logout",
              "logoutFromAllDevices": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/customer/logOutFromAllDevice",
              "fetchMWList": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/marketWatch/fetchMWList",
              "fetchMWScrips": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/marketWatch/fetchMWScrips",
              "addScripToMW": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/marketWatch/addScripToMW",
              "deleteMWScrip": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/marketWatch/deleteMWScrip",
              "scripDetails": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/ScripDetails/getScripQuoteDetails",
              "positions": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/positionAndHoldings/positionBook",
              "holdings": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/positionAndHoldings/holdings",
              "sqrOfPosition": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/positionAndHoldings/sqrOofPosition",
              "fetchOrder": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/placeOrder/fetchOrderBook",
              "fetchTrade": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/placeOrder/fetchTradeBook",
              "exitBracketOrder": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/placeOrder/exitBracketOrder",
              "modifyOrder": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/placeOrder/modifyOrder",
              "cancelOrder": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/placeOrder/cancelOrder",
              "orderHistory": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/placeOrder/orderHistory",
              "getRmsLimits": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/limits/getRmsLimits",
              "createWsSession": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/ws/createSocketSess",
              "history": f"https://a3.aliceblueonline.com/rest/AliceBlueAPIService/api/chart/history",
              "master_contract": "https://v2api.aliceblueonline.com/restpy/contract_master?exch={exchange}",
              "ws": "wss://ws2.aliceblueonline.com/NorenWS/"
              }
    api_key = 'ZlXqEZrIKcOzkcr'
    api_secret = 'yCzXoqUirUVZWxNQuImprDYakAOIkGjehsGIXLvhhcVykLNfiZDRusYEVtPEYBsXDivAsaBZsOaRqSdFuxMxAsuXiHHCFjWYqTbC'
    password = 'Aveeno@975ml'
    user_id = '345397'
    twofa = 'QUDKUWDPGSGHDLTHBYMPSNAYPNGNSTPF'
    by = '1995'
    # access_token = AliceBlue.login_and_get_sessionID(username=user_id, password=password, twoFA='1995',app_id=api_key, api_secret=api_secret)

    data = {"userId": user_id}
    header = {"Content-Type": "application/json"}

    r = requests.post(__urls['getEncKey'], headers=header, json=data)
    encKey = r.json()["encKey"]


    checksum = CryptoJsAES.encrypt(password.encode(), encKey.encode())
    checksum = checksum.decode("utf-8")
    data = {"userId": user_id,
            "userData": checksum}
    r = requests.post(__urls["webLogin"], json=data)



    data = {"answer1": by,
            "sCount": "1",
            "sIndex": "1",
            "userId": user_id,
            "vendor": api_key}
    r = requests.post(__urls["twoFA"], json=data)
    print(r)
    isAuthorized = r.json()['isAuthorized']
    authCode = parse_qs(urlparse(r.json()["redirectUrl"]).query)['authCode'][0]



