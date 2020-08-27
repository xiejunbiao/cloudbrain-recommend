# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 17:52:16 2020

@author: zhaizhengyuan
"""
#from .import dataBaseHandle
import sys
import time
import datetime
sys.path.append("../") ## 会从上一层目录找到一个package
#sys.path.append("/opt/ado-services/cloudbrain-recommend/cloudbrain-recommend/hot_sale_rec-recommend/hotsale_offline_com/") ## 服务器上改成绝对路径

from hotsale_offline_com.utils.dataBaseHandle import readMysqlmultiPd,update_table_data
from hotsale_offline_com.owners_rec_spus import get_owners_rec_spus_dict
from hotsale_offline_com.table_insert_data import gen_insert_data
from hotsale_offline_com.all_spus_keywords import read_all_goods_info

from hotsale_offline_com.utils.configInit import ConfigValue

# argvs = 'D:\hisense'
# configv = ConfigValue(argvs)
# config_result={}
# config_result['mysql_1'] = configv.get_config_values('mysql_1')
# config_result['mysql_2'] = configv.get_config_values('mysql_2')
# config_result['dbTable'] = configv.get_config_values('dbTable')
def str_gen(list):

    genstr = ""
    for i in range(len(list)):
        genstr = genstr+'\'%s\','
    genstr = '('+genstr[:-1]+')'
    return genstr

def find_new_owner_list(dbTable,config_result,begin_time,end_time):

    mysql = "select owner_code from "+dbTable+" where first_cate is not null and second_cate is not null and paid_time between '%s'\
             and '%s'" % (begin_time, end_time)
    dataDf = readMysqlmultiPd(mysql, config_result['mysql_2'])
    mysql = "select owner_code,count(owner_code) as count from "+\
            dbTable+" group by owner_code having count >3"
    dataDf1 = readMysqlmultiPd(mysql, config_result['mysql_2'])
    new_owner_list = list(set(dataDf['owner_code'].tolist()) & set(dataDf1['owner_code'].tolist()))
    return new_owner_list

def update_offline_rec_table(config_result,begin_time,end_time):

    order_table = config_result['dbTable']['order_table']
    rec_table = config_result['dbTable']['rec_table']
#########离线建立用户历史订单表用于热卖推荐开发
    new_owner_list = find_new_owner_list(order_table,config_result,begin_time,end_time)
    #new_owner_list = ['*20180225008687']
    if new_owner_list!=[]:
##################离线建立所有用户的热卖推荐结果表#####################
        ##读取新用户下单数据
        list_str = str_gen(new_owner_list)
        query_sql = "select * from "+order_table+" where first_cate is not null and second_cate is not null and owner_code IN "+list_str % tuple(new_owner_list)
        dataDf1 = readMysqlmultiPd(query_sql,config_result['mysql_2'])
        # ##读取用户下单的一级商品类别
        query_sql = "select first_cate from "+order_table+" where first_cate is not null and second_cate is not null and owner_code IN " + list_str % tuple(new_owner_list)
        first_data = readMysqlmultiPd(query_sql, config_result['mysql_2'])
        first_list = list(set(first_data["first_cate"].tolist()))
        # first_list = [str(x) for x in first_list]
        # ##读取所有相关一级类别商品信息
        # query_sql = "select * from bd_goods_spu_for_filter where first_cate \
        # IN (%s)" % ','.join(first_list)
        # print(query_sql)
        # dataDf = readMysqlmultiPd(query_sql, 'our_database')

        ##读取所有商品信息表
        dataDf = read_all_goods_info(config_result['mysql_2'])
        dataDf = dataDf[dataDf['first_cate'].isin(first_list)]
        ##根据下单数据表计算用户的推荐结果字典
        ##字典的主键是用户id
        ##字典的键值是用户的推荐结果字典
        ##推荐结果字典中键是一级类别，值是推荐结果列表(spu_code list)
        owners_rec_codes_dict = get_owners_rec_spus_dict(dataDf,dataDf1)
        ##将推荐结果字典存转化成可以插入表格中的数据(元组列表)
        ##表格第1列是ownerid,第2列数据是一级类别代码，第3列数据是spu_colde_list的字符串形式


        insert_data = gen_insert_data(owners_rec_codes_dict)
        #print(insert_data[0])
        #insert_data[2] = ('*20180225008687',100002,'sssssss')
        #insert_data[0] = ('*20180225008687',100164,'389771666843762688')
        updatesql = "INSERT INTO " + rec_table + " VALUES (0,%s, %s, %s) " \
              "ON DUPLICATE KEY UPDATE `owner_code` = VALUES(`owner_code`), `first_cate` = VALUES(`first_cate`) , spu_code_list = VALUES(`spu_code_list`)"
        ##更新数据
        #insert_table(updatesql,insert_data,'our_database')
        update_table_data(updatesql,insert_data,config_result['mysql_2'])
        print("%d data is updated." % len(insert_data))
    else:
        print("no new owner order.")

def update_offline_table(config_result):

    current_time = datetime.datetime.now()
    t0 = time.time()
    begin_time = current_time + datetime.timedelta(hours=-1)
    begin_time = begin_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    update_offline_rec_table(config_result,begin_time,end_time)
    t1 = time.time()
    print("Time consumed(s): ", t1 - t0)
    print("当前更新时间：%s" % time.ctime())
    # 2s检查一次
    #time.sleep(60)
