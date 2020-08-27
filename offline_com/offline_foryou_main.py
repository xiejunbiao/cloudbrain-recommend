# @author : zhaizhengyuan
# @datetime : 2020/8/13 17:18
# @file : offline_foryou_main.py
# @software : PyCharm

"""
文件说明：

"""
from foryou_offline_com.ini_and_update import start
from foryou_offline_com.utils.configInit import config_dict, get_logger

if __name__ == '__main__':
    two_mysql = config_dict
    logger = get_logger()
    start(two_mysql, logger)
