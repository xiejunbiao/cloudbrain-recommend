# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 12:15:40 2020

@author: lijiangman
"""

import configparser
import os
import sys
import getopt


class ConfigValue:
    # 项目路径
    # rootDir = os.path.split(os.path.realpath(__file__))[0]
    # config.ini文件路径
    # configFilePath = os.path.join(rootDir, 'config.ini')
    def __init__(self, rootdir):
        configfilepath = os.path.join(rootdir, 'config.ini')
        self.config = configparser.ConfigParser()
        self.config.read(configfilepath)
        # print(configFilePath)

    def get_config_values(self, section_argv):
        """
        根据传入的section获取对应的value
        :param section_argv: ini配置文件中用[]标识的内容
        :return:
        """
        # return config.items(section=section)
        if section_argv == 'dbTable':
            arglist = ['order_table', 'rec_table', 'filter_table']
        else:
            arglist = ['ip', 'user', 'password', 'db', 'port']

        config_result = {}
        for option in arglist:
            if option == 'envIp':
                section = 'storeinterface'
            else:
                section = '%s' % section_argv
            config_result[option] = self.config.get(section=section, option=option)

        return config_result


def para_set(argv):
    try:
        opts, args = getopt.getopt(argv, "-c:-l", ["conf=", "log="])
    except getopt.GetoptError:
        print('Please input correct params')
        sys.exit()
    for opt, arg in opts:
        if opt in ("-c", "--conf"):
            config = arg
            return config
        elif opt in ("-l", "--log"):
            log = arg
            return log


# #并且在def外面全局变量，初始化一次数据库配置

# rootDir='/opt/recommendhotgoods'#服务器配置
# rootDir='D:\hisense1'
# cv=ConfigValue(rootDir)
# config_result=cv.get_config_values()##全局变量，运行一次，数据库配置。放到内存里面

# if __name__=='__main__':
# cv=ConfigValue(u'D:\hisense')
# config_result=cv.get_config_values()
# print('')
