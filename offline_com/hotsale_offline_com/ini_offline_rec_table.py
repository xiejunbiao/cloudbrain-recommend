# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 17:52:16 2020

@author: zhaizhengyuan
"""
#from .import dataBaseHandle
import sys
import time
sys.path.append("../") ## 会从上一层目录找到一个package
#sys.path.append("/opt/ado-services/cloudbrain-recommend/cloudbrain-recommend/offline_com/hotsale_offline_com/") ## 服务器上改成绝对路径
from hotsale_offline_com.utils.dataBaseHandle import readMysqlmultiPd,insert_table
from hotsale_offline_com.owners_rec_spus import get_owners_rec_spus_dict
from hotsale_offline_com.table_insert_data import gen_insert_data
from hotsale_offline_com.all_spus_keywords import read_all_goods_info
from hotsale_offline_com.utils.configInit import ConfigValue

# argvs = 'D:\hisense'
# config_result = {}
# configv = ConfigValue(argvs)
# config_result['mysql_1'] = configv.get_config_values('mysql_1')
# config_result['mysql_2'] = configv.get_config_values('mysql_2')
# config_result['dbTable'] = configv.get_config_values('dbTable')
def build_offline_table(config_result):

    order_table = config_result['dbTable']['order_table']
    rec_table = config_result['dbTable']['rec_table']

##################离线建立所有用户的热卖推荐结果表#####################
    ##读取所有商品信息表
    t0 = time.time()
    dataDf = read_all_goods_info(config_result['mysql_2'])

    ##读取用户下单数据表
    query_sql = "select * from "+order_table+" where first_cate is not null and second_cate is not null"
    dataDf1 = readMysqlmultiPd(query_sql,config_result['mysql_2'])
    ##根据下单数据表计算用户的推荐结果字典
    ##字典的主键是用户id
    ##字典的键值是用户的推荐结果字典
    ##推荐结果字典中键是一级类别，值是推荐结果列表(spu_code list)
    owners_rec_codes_dict = get_owners_rec_spus_dict(dataDf,dataDf1)

##将推荐结果字典存转化成可以插入表格中的数据(元组列表)
##表格第1列是ownerid,第2列数据是一级类别代码，第3列数据是spu_colde_list的字符串形式
    insert_data = gen_insert_data(owners_rec_codes_dict)
    insertsql = "INSERT INTO " + rec_table + "(owner_code,first_cate,spu_code_list) VALUES (%s,%s,%s)"
    updatesql = "INSERT INTO " + rec_table + " VALUES (0,%s, %s, %s) " \
                                             "ON DUPLICATE KEY UPDATE `owner_code` = VALUES(`owner_code`), `first_cate` = VALUES(`first_cate`) , spu_code_list = VALUES(`spu_code_list`)"
    t1 = time.time()
    insert_table(rec_table,insertsql,updatesql,insert_data,config_result['mysql_2'])
    #print("Time consumed(s): ", t1 - t0)
    print("初始化推荐结果表完成")

#build_offline_table(config_result)
