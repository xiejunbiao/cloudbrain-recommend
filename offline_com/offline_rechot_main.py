# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 15:25:02 2020

@author: zhaizhengyuan
"""

import os
import sys
import traceback
#####
import logging
from multiprocessing import Process
import time
import datetime
import schedule

# from logging.handlers import TimedRotatingFileHandler
logging.basicConfig()

######

#sys.path.append("../") ## 会从上一层目录找到一个package
sys.path.append("/opt/ado-services/cloudbrain-recommend/cloudbrain-recommend/offline_com/")  ## 服务器上改成绝对路径
from hotsale_offline_com.utils.logHandle import SafeFileHandler
from hotsale_offline_com.utils.configInit import ConfigValue
from hotsale_offline_com.utils.configInit import para_set
from hotsale_offline_com.ini_offline_rec_table import build_offline_table
from hotsale_offline_com.update_offline_rec_table import update_offline_table

# from timeClassModul import Time_log_info##我自己定义的时间类
# g_log_prefix = '../log/rela_baike_tornado.'
# g_log_prefix = './logs/rechot_goods.'##目录
#argvs = 'D:\hisense'
#config_result = {}
#configv = ConfigValue(argvs)

argv = sys.argv[1:]
argvs = para_set(argv)
config_result = {}
configv = ConfigValue(argvs)

config_result['mysql_1'] = configv.get_config_values('mysql_1')
config_result['mysql_2'] = configv.get_config_values('mysql_2')
config_result['dbTable'] = configv.get_config_values('dbTable')

# g_log_prefix = '/var/log/recommendhotgoods/rechot_goods.'#绝对路径
####
##为了定义日志的时间格式，写一个时间类的模块
# 以下定义了一个类，类名为Person_info，里面有两个参数，分别是name和age
class Time_log_info:
    def __init__(self, hour, minute, second):
        self.hour = hour
        self.minute = minute
        self.second = second


def getLogger():
    # strPrefix = "%s%d%s" % (strPrefixBase, os.getpid(),".log")##进程的pid命名日志
    logger = logging.getLogger("RECHOT_GOODS")
    logger.propagate = False
    # handler = TimedRotatingFileHandler(strPrefix, 'H', 1)
    ##将http访问记录，程序自定义日志输出到文件，按天分割，保留最近30天的日志。
    # 使用TimedRotatingFileHandler处理器
    #    handler = TimedRotatingFileHandler(strPrefix, when="d", interval=1, backupCount=60)##d表示按天
    #    interval 是间隔时间单位的个数，指等待多少个 when 的时间后 Logger 会自动重建新闻继续进行日志记录
    #    handler = TimedRotatingFileHandler(strPrefix, when="midnight", interval=0, backupCount=60)##
    #    atTime=Time_log_info(23,59,59)
    #    handler = TimedRotatingFileHandler(strPrefix, when="midnight", interval=0, backupCount=60, atTime=atTime)##
    # 实例化TimedRotatingFileHandler
    # interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
    # S 秒、 M 分、 H 小时、 D 天、W 每星期（interval==0时代表星期一）、 midnight 每天凌晨
    ##
    # handler=SafeFileHandler(filename=strPrefix,mode='a')
    #    handler.suffix = "%Y%m%d_%H%M%S.log"
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.INFO)
    # handler.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger

def ini_rec_task():
    print('------------------------------------------------------------------------')
    print('定时初始化任务:每天凌晨4:00执行一次')
    t0=time.time()
    build_offline_table(config_result)
    t1=time.time()
    print("Time consumed(s): ", t1 - t0)
    print('------------------------------------------------------------------------')

def update_rec_task():
    print('************************************************************************')
    print('定时更新任务:每隔20分钟更新一次')
    t0 = time.time()
    try:
        update_offline_table(config_result)
    except:
        print("{}".format(traceback.format_exc()))
    t1 = time.time()
    print("Time consumed(s): ", t1 - t0)
    print('*************************************************************************')


if __name__ == "__main__":
    build_offline_table(config_result)
    schedule.every().day.at('04:00').do(ini_rec_task)
    schedule.every(20).minutes.do(update_rec_task)
    while True:
        schedule.run_pending()
        time.sleep(5)
    # build_offline_table(config_result)
    # update_table(config_result)
    # start()

