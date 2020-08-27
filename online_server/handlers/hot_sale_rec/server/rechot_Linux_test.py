# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 16:35:53 2019

@author: lijiangman
"""
import json
import sys
sys.path.append("../") ## 会从上一层目录找到一个package
import requests
import time
################post
url = "http://10.18.222.105:6602/cloudbrain-recommend/recommends/gethotsalegoods"
#url = "http://123.103.112.76:6602/cloudbrain-recommend/recommends/gethotsalegoods"
#url = "http://10.18.226.62:6602/cloudbrain-recommend/recommends/gethotsalegoods"
#url = "http://10.18.226.25:8890/rechotgoods"


#url = "http://10.18.226.25:8891/rechotgoods"  ##failed:'data' 错误接口
# url = "https://10.18.226.25:8890/rechotgoods"

spuDict={
    "page": "1",
    "rows": "10",
    "sortMethod":"null",##默认不传这个sortMethod
    "spuCodeList":['367517642877882369','314377269667930112'],
    # "spuCodeList":[],
    "salePercent":"10"#销量百分比
}
spuDict={
    "page": "1",
    "rows": "10",
    "ownerCode": "2020033013381501443",
    "areaCode": "A2018012300015",
    "cateCode": "100001",
    # "sortMethod":"null",##默认不传这个sortMethod
    "sortMethod":"0",
#    spuCodeList":['314377368200519680','314377269667930112'],
    # "spuCodeList": ["371456364033470464", "391406725397606400"],
    "spuCodeList":[],
    "salePercent":"30"#销量百分比
}
t0 = time.time()

aheaders={'Content-Type':'application/json'}
result = requests.post(url, data = json.dumps(spuDict))##我省略了, headers=aheaders
# result = requests.post(url, headers=aheaders, json = json.dumps(spuDict))##我省略了, headers=aheaders
t1 = time.time()
print("time consumed: ",t1-t0)
print(result.text)
# print(result.headers)
# text=result.text
# print(text+'\n', type(text))
# print(result.headers, result.text)
# spuCodeList=json_text['data']
# print(json_text)