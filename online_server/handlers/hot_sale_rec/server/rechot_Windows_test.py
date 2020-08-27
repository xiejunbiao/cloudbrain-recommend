# -*- coding: utf-8 -*-

import json

import sys

sys.path.append("../")  ## 会从上一层目录找到一个package

from Owners_RecV2.rec_for_hotsale import rechot_main  # 模块名字不能有小数点
from Owners_RecV2.queryReturn import RecHotUP
from Owners_RecV2.utils.configInit import ConfigValue
import time
import multiprocessing


# import pandas as pd

# 这个版本更改了销量排序逻辑，需要测试一下是否正常排序

# 用__name__==这个测试一下排序结果

# 下一版本是用户画像，销量top10的这个规则应该不适用了！！？？？问炯丽
# 设置一个商品总量百分比？？？
# 置顶商品要过滤库存
# 总数需保持百分比不变
rootDir='D:\workbench\django1_11\hotsale_online_server\hotsale_online_server\settings'
config_result = {}
configv = ConfigValue(rootDir)
config_result['redis'] = configv.get_config_values('redis')
config_result['mysql_2'] = configv.get_config_values('mysql_2')

def rec_test():

    t0 = time.time()
    # page_json = search_main(1, 20, areaCode, cateCode, sortMethod)
    # spuDict={
    #     "page": "1",
    #     "rows": "10",
    #     "areaCode":"2018012300001",
    #     "cateCode":"100001",
    #     # "sortMethod":"null",##默认不传这个sortMethod
    #     "sortMethod":"0",
    # #    "spuCodeList":['314377368200519680','314377269667930112'],
    #     "spuCodeList": ["371456364033470464", "391406725397606400"],
    #     # "spuCodeList":[],
    #     "salePercent":"99"#销量百分比
    # }
    spuDict = {
        "page": "1",
        "rows": "20",
        "ownerCode":"1586484620",#0265311327
        "areaCode": "A2018012300015",
        "cateCode": "100001",
        'secondCateCode': '2412463',
        "sortMethod": "0",
        "spuCodeList": ['367518499849687041'],
        "salePercent": "50"  # 销量百分比
    }

    page_json = rechot_main(spuDict,config_result)

    t1 = time.time()
    time_used = t1 - t0
    print("Time consumed(s): ", time_used)

    page_dict = json.loads(page_json)
    print(page_dict)
    spu_codes = page_dict['data']['spuCodeList']
    #print(spu_codes.index('395400417195589632'))
    print('len:', len(spu_codes))
    # #还原
    # dataDf=dataDf.reset_index()
    # print(dataDf)
    # print(dataDf['spu_code'])
    if time_used > 6:
        print('time used is %s' % (time_used))
        sys.exit(0)

    return spu_codes
    # return dataDf_ori


def multiProcess():
    n_proc = 4
    llist = []
    res_list = []
    res_list2 = []
    pool = multiprocessing.Pool(processes=n_proc)
    #    for i in range(4):
    index_list = [0, 1, 2, 3] * 10
    for i in index_list:
        print(i)
        #        apply_async 是异步非阻塞的。
        #        意思就是：不用等待当前进程执行完毕，随时根据系统调度来进行进程切换。
        result = pool.apply_async(rec_test, args=())
        #        print(result.get())
        #        llist.append(result)
        res_list.append(result)
    #        res_list.append(result.get())
    pool.close()
    pool.join()
    ##必须运行下面的语句，否则时间不会变短！！
    for i in range(len(index_list)):
        res = res_list.pop(0)
        res_list2.append(res.get())
    #    print(res_list)
    return res_list2


if __name__ == "__main__":
    # for i in range(100):
    # dataDf=rec_test()
    # res_list2=multiProcess()
    dataDf = rec_test()

    ##单次请求库存接口也存在连接错误？





