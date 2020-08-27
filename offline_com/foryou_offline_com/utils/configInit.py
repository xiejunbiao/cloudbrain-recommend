#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time      :2020/8/3 16:41
# @Author    :XieYuHao


import configparser
import os
import sys
import logging

argv = sys.argv[1:]


def get_conf_dict(conf_dir_path):
    conf_file_path = os.path.join(conf_dir_path, 'config.ini')
    conf_obj = configparser.ConfigParser()
    conf_obj.read(conf_file_path)
    conf_dict = {section: dict(conf_obj.items(section)) for section in conf_obj.sections()}
    # port配置转化为Int类型方便连接mysql
    for k, v_dict in conf_dict.items():
        if 'port' in v_dict:
            v_dict['port'] = int(v_dict['port'])
    return conf_dict


def get_logger(logger_name=None):
    logger = logging.getLogger(logger_name)
    logger.propagate = False
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.INFO)
    # handler.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger


def log_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)
    handler = logging.FileHandler("log.log")
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    # logger.info("Start print log")
    # logger.debug("Do something")
    # logger.warning("Something maybe fail.")
    # logger.info("Finish")
    return logger


# config_dict = get_conf_dict("D:\hisense")
config_dict = get_conf_dict(argv[1])
# print(config_dict["mysql_1"])
