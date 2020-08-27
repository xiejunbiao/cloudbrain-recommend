# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 14:03:22 2020
@author: zhaizhengyuan
计算最新下单、收藏、评价的用户列表
以用于后续增量更新用户的个性化商家列表
"""
import sys
from hotshop_offline_com.utils.dataBaseHandle import DatabaseHandle
# from hotshop_offline_com.utils.config import ModifyConfFile, log_logger
sys.path.append("..")

# logger = log_logger()
# mcf = ModifyConfFile(logger)
# two_mysql = mcf.return_argv()


class FindNewOwner(object):
    """
        发现新用户类：
        根据历史下单数据，获取符合时间条件的下单用户列表
        根据历史收藏数据，获取符合时间条件的收藏用户列表
        根据历史评价数据，获取符合时间条件的评价用户列表
        最后得到一个总的用户列表
     """

    def __init__(self, two_mysql, logger, begin_time, end_time):
        self.read_hisense = DatabaseHandle(two_mysql["hisense_mysql"], logger)
        self.read_sqyn = DatabaseHandle(two_mysql["sqyn_mysql"], logger)
        self.begin_time = begin_time  # 时间条件的起始时间
        self.end_time = end_time  # 时间条件的终止时间
        self.new_order_owner = []
        self.new_collect_owner = []
        self.new_eval_owner = []
        self.new_owner_list = None
        self.total_new_owner()

    # 发现新的订单用户列表
    def find_new_order_owner(self):
        mysql = '''
            SELECT 
                owner_code 
            FROM 
                cb_owner_buy_goods_info 
            WHERE 
                paid_time 
            BETWEEN 
                '%s'
            AND 
                '%s'
            ''' % (self.begin_time, self.end_time)
        order_data = self.read_sqyn.read_mysql_multipd(mysql)
        if len(order_data) != 0:
            self.new_order_owner = order_data["owner_code"].tolist()

    # 发现新的收藏用户列表
    def find_new_collect_owner(self):
        mysql = '''
            SELECT 
                user_code 
            FROM 
                user_collect_record 
            WHERE 
                created_time 
            BETWEEN 
                '%s'
            AND 
                '%s'
            ''' % (self.begin_time, self.end_time)
        collect_data = self.read_hisense.read_mysql_multipd(mysql)
        if len(collect_data) != 0:
            self.new_collect_owner = collect_data["user_code"].tolist()

    # 发现新的评价用户列表
    def find_new_eval_owner(self):
        mysql = '''
            SELECT 
                owner_code 
            FROM 
                goods_spu_eval 
            WHERE 
                eval_time 
            BETWEEN 
                '%s'
            AND 
                '%s'
            ''' % (self.begin_time, self.end_time)
        eval_data = self.read_hisense.read_mysql_multipd(mysql)
        if len(eval_data) != 0:
            self.new_eval_owner = eval_data["owner_code"].tolist()

    # 总新用户列表
    def total_new_owner(self):
        self.find_new_order_owner()
        self.find_new_collect_owner()
        self.find_new_eval_owner()
        total_new_owner_list = self.new_order_owner + \
            self.new_collect_owner + \
            self.new_eval_owner
        self.new_owner_list = list(set(total_new_owner_list))
        return self.new_owner_list


if __name__ == '__main__':
    pass
