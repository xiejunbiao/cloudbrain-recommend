# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 11:10:35 2020
@author: zhaizhengyuan
计算用户的个性化商家排序模块
用于根据用户的历史下单、收藏、评价行为计算用户的个性化商家列表
以区分按销量或者评价排序得到的商家列表
"""
import sys
import datetime
# import time
from hotshop_offline_com.utils.dataBaseHandle import DatabaseHandle
# from hotshop_offline_com.utils.config import ModifyConfFile, log_logger
# from hotshop_offline_com.get_shop_list import HotShopRead

sys.path.append("..")

# logger = log_logger()
# mcf = ModifyConfFile(logger)
# two_mysql = mcf.return_argv()
# hsr = HotShopRead(two_mysql, logger)


class DataRead(object):
    """
       基础数据读取类：
       读取sqyn库中的历史下单表，获取历史下单数据和下单用户列表
       读取hisense库中的历史收藏表，获取历史收藏数据和收藏用户列表
       读取hisense库中的历史评价表，获取历史评价数据和评价用户列表
       最后得到一个总的用户列表
    """

    def __init__(self, two_mysql1, logger1, shop_list):
        self._logger = logger1
        self.hisense_mysql = two_mysql1['hisense_mysql']
        self.sqyn_mysql = two_mysql1['sqyn_mysql']
        self.read_hisense = DatabaseHandle(self.hisense_mysql, logger1)
        self.read_sqyn = DatabaseHandle(self.sqyn_mysql, logger1)
        self.shop_list = shop_list  # shop_list为通过ShopRead类得到的排序后的商家排序列表
        self.order_data = None
        self.total_shop_data = None
        self.collect_data = None
        self.spu_eval_data = None
        self.order_eval_data = None
        self.owner_list = None
        self.total_owner()

    # 读取用户的历史订单数据，返回历史订单用户列表
    def read_order_table(self):
        mysqlcmd = '''
            SELECT 
                owner_code, 
                shop_code, 
                count(owner_code) AS count 
            FROM 
                cb_owner_buy_goods_info 
            GROUP BY 
                owner_code, 
                shop_code
        '''
        self.order_data = self.read_sqyn.read_mysql_multipd(mysqlcmd)
        order_owner_list = self.order_data["owner_code"].tolist()
        return order_owner_list

    # 读取用户的历史收藏数据，返回历史收藏用户列表
    def read_user_collect(self):
        mysqlcmd = '''
            SELECT 
                user_code, 
                content_code 
            FROM 
                user_collect_record 
            WHERE 
                collect_type = '02'
        '''
        self.collect_data = self.read_hisense.read_mysql_multipd(mysqlcmd)
        collect_owner_list = self.collect_data["user_code"].tolist()
        return collect_owner_list

    # 读取用户的历史评价数据，返回历史评价用户列表
    def read_spu_eval(self):
        mysqlcmd = '''
            SELECT 
                owner_code, 
                shop_code, 
                eval_level, 
                count(owner_code) AS count 
            FROM 
                goods_spu_eval 
            GROUP BY 
                owner_code, 
                shop_code, 
                eval_level
        '''
        self.spu_eval_data = self.read_hisense.read_mysql_multipd(mysqlcmd)
        spu_eval_owner = self.spu_eval_data["owner_code"].tolist()
        return spu_eval_owner

    # 读取用户的历史订单评价，返回历史订单评价用户列表
    # 此方法暂时不用
    def read_order_eval(self):
        mysqlcmd = '''
            SELECT 
                c.owner_code, 
                a.shop_code, 
                a.serve_eval_level, 
                count(c.owner_code) AS count 
            FROM 
                goods_child_orders_eval a, 
                sale_order_sku b, 
                sale_order c 
            WHERE 
                a.order_code = b.sub_bill_no 
            AND 
                b.bill_no = c.bill_no 
            GROUP BY 
                c.owner_code, 
                a.shop_code, 
                a.serve_eval_level
        '''
        self.order_eval_data = self.read_hisense.read_mysql_multipd(mysqlcmd)
        order_eval_owner = self.order_eval_data["owner_code"].tolist()
        return order_eval_owner

    # 返回总的用户列表，用于后续的个性化商家计算
    def total_owner(self):
        order_owner_list = self.read_order_table()
        collect_owner_list = self.read_user_collect()
        spu_eval_owner_list = self.read_spu_eval()
        self.owner_list = list(set(order_owner_list +
                                   collect_owner_list +
                                   spu_eval_owner_list))


class OwnerShop(DataRead):
    """
        用户店铺类：其父类是DataRead
        计算用户的历史下单商家数量，获取所有商家的历史下单评分
        计算用户的历史收藏商家数量，获取所有商家的历史收藏评分
        计算用户的历史评价商家数量，获取所有商家的历史评价评分
        最后得到一个总的商家评分字典
     """

    def __init__(self, two_mysql2, logger2, shop_list, new_owner_list):
        super().__init__(two_mysql2, logger2, shop_list)  # 父类DataRead的初始化
        self.owner_shop_dict = {}
        self._new_owner_list = new_owner_list
        self._owner_buy_shop_score = None
        self._owner_eval_shop_score = None
        self._owner_collect_shop_score = None
        self._order_eval_shop_score = None
        self._owner_shop_score_dict = {}

    # 计算用户-商家的历史下单评分
    def _owner_buy_shop(self, owner_code, shop_code):
        # sum1 = self.order_data[
        #     self.order_data.owner_code == owner_code]["count"].sum()
        try:
            # if self.order_data[
            #     (self.order_data.owner_code == owner_code)
            #     & (self.order_data.shop_code == shop_code)
            # ]["count"].values[0] == sum1:
            self._owner_buy_shop_score.append(
                (self.order_data[
                    (self.order_data.owner_code == owner_code)
                    & (self.order_data.shop_code == shop_code)
                    ]["count"].values[0]) * 0.05)
            # else:
            #     self._owner_buy_shop_score.append(
            #         (self.order_data[
            #             (self.order_data.owner_code == owner_code)
            #             & (self.order_data.shop_code == shop_code)
            #             ]["count"].values[0]) / sum1)

        except IndexError:
            self._owner_buy_shop_score.append(0)

    # 计算用户-商家的历史收藏评分
    def _owner_collect_shop(self, owner_code, shop_code):
        self._owner_collect_shop_score.append(len(
            self.collect_data[
                (self.collect_data.user_code == owner_code)
                & (self.collect_data.content_code == shop_code)
                ]) * 10)

    # 根据用户对店铺的历史评价数据，计算评价等级得分
    # 按用户对商家的评价等级进行评分
    # 评价等级为5,4,3的为正得分，等级越低权重越小
    # 评价等级为2,1的为负得分，等级越低权重越高
    def _get_owner_eval_score(self, data):
        self.temp_score = 0
        for i in range(0, len(data)):
            if data.iloc[[i]].values[0][2] == 5:
                self.temp_score += data.iloc[[i]].values[0][3] * 0.01
            elif data.iloc[[i]].values[0][2] == 4:
                self.temp_score += data.iloc[[i]].values[0][3] * 0.01 * 0.1
            elif data.iloc[[i]].values[0][2] == 3:
                self.temp_score += data.iloc[[i]].values[0][3] * 0.01 * 0.01
            # elif data.iloc[[i]].values[0][2] == 2:
            #     self.temp_score -= data.iloc[[i]].values[0][3] * 0.01
            # temp_score -= self._owner_eval_shop_data[
            #     self._owner_eval_shop_data.eval_level == 2]["count"].values[0] * 0.01
            else:
                self.temp_score -= data.iloc[[i]].values[0][3] * 0.5
            return self.temp_score

    # 计算用户-商家的历史评价评分，需要调用_get_owner_eval_score方法
    def _owner_eval_shop(self, owner_code, shop_code):
        self._owner_eval_shop_data = self.spu_eval_data[
            (self.spu_eval_data.owner_code == owner_code)
            & (self.spu_eval_data.shop_code == shop_code)
            ]
        if self._owner_eval_shop_data.empty:

            self._owner_eval_shop_score.append(0)
        else:
            temp_score = self._get_owner_eval_score(self._owner_eval_shop_data)
            # self._owner_eval_shop_score.append(
            #             #     (self._owner_eval_shop_data["eval_level"] *
            #             #      self._owner_eval_shop_data["count"]).sum() * 0.01)
            self._owner_eval_shop_score.append(temp_score)

    # 根据用户的历史下单、收藏、评价行为，得到对应的商家列表，加快计算过程
    def _get_owner_shop_list(self, owner_code):
        owner_buy_shop_list = self.order_data[
            self.order_data.owner_code == owner_code]["shop_code"].tolist()
        owner_collect_shop_list = self.collect_data[
            self.collect_data.user_code == owner_code]["content_code"].tolist()
        owner_eval_shop_list = self.spu_eval_data[
            self.spu_eval_data.owner_code == owner_code]["shop_code"].tolist()
        self.owner_shop_list = list(set(owner_buy_shop_list +
                                        owner_collect_shop_list +
                                        owner_eval_shop_list))

    # 计算一个用户的个性化商家排序字典，
    # 键是用户，值是排序后的商家列表（字符串形式，方便存储mysql）
    # 需要先计算用户的商家得分字典，键是shop_code，值是用户对店铺的打分
    def _total_shop_score(self, owner_code):
        self.total_score = [self._owner_buy_shop_score[i]
                            + self._owner_eval_shop_score[i]
                            + self._owner_collect_shop_score[i]
                            for i in range(0, len(self._owner_buy_shop_score))]
        self._owner_shop_score_dict = dict(zip(self.shop_list, self.total_score))

        self._owner_shop_score_dict = dict(tuple(
            sorted(self._owner_shop_score_dict.items(),
                   key=lambda x: x[1], reverse=True)))

        # print(self._owner_shop_score_dict)
        # temp_list = list(self._owner_shop_score_dict.keys()) + self.shop_list
        # self.owner_sort_shop_list = list(set(temp_list))
        # self.owner_sort_shop_list.sort(key=temp_list.index)
        self.owner_sort_shop_list = list(self._owner_shop_score_dict.keys())
        self.owner_shop_dict[owner_code] = ",".join(self.owner_sort_shop_list)

    # 计算所有用户的个性化商家排序字典
    # 为了能融合定时的增量更新任务
    # 需要判断一下是计算新增用户，还是全量用户
    def total_owner_score_dict(self):

        if self._new_owner_list:
            owner_list = self._new_owner_list
        else:
            owner_list = self.owner_list
        # owner_list = ["*2020010309123194740","2020070716542815665"]
        for owner_code in owner_list:
            self._owner_buy_shop_score = []
            self._owner_eval_shop_score = []
            self._owner_collect_shop_score = []
            print(owner_code)
            # self._get_owner_shop_list(owner_code)
            # print(self.shop_list)
            for shop_code in self.shop_list:
                self._owner_buy_shop(owner_code, shop_code)
                self._owner_collect_shop(owner_code, shop_code)
                self._owner_eval_shop(owner_code, shop_code)
            self._total_shop_score(owner_code)
        self.owner_shop_dict["000000"] = ",".join(self.shop_list)

        return self.owner_shop_dict


# 增加插入数据的时间字段内容
def add_time(data):
    temp_time = datetime.datetime.now()
    data0 = [(list(data)[i]) + (temp_time,)
             for i in range(0, len(data))]
    return data0


# 将计算出来的个性化商家排序结果存储到mysql表
# mysql表没有数据则插入，有则直接进行更新
def save_owner_shops(two_mysql3, logger3, save_data1):
    in_sql = """
        INSERT INTO cb_hotshop_owner_rec_shops (
            owner_code,
            shop_code_list,
            updated_time
        )
        VALUES
        (% s, % s, s %)
    """
    # update_sql = "INSERT INTO cb_hotshop_owner_rec_shops" + \
    #              " VALUES (0, %s, %s, %s) ON DUPLICATE KEY UPDATE " \
    #              "`owner_code` = VALUES(`owner_code`), " \
    #              "`shop_code_list` = VALUES(`shop_code_list`), " \
    #              "`updated_time` = VALUES(`updated_time`)"
    update_sql = """
        INSERT INTO cb_hotshop_owner_rec_shops (
            owner_code,
            shop_code_list,
            updated_time
        )
        VALUES (% s, % s, % s) 
        ON DUPLICATE KEY UPDATE 
        `owner_code` = VALUES(`owner_code`),
        `shop_code_list` = VALUES(`shop_code_list`),
        `updated_time` = VALUES(`updated_time`)
    """
    db_table = "cb_hotshop_owner_rec_shops"
    sqyn_write = DatabaseHandle(two_mysql3['sqyn_mysql'], logger3)
    sqyn_write.insert_table(in_sql, update_sql, db_table, save_data1)


# shop_list = hsr.sort_shop_list
# print(shop_list)
# os1 = OwnerShop(two_mysql, logger, shop_list, [])
# owner_shop_dict = os1.total_owner_score_dict()
# save_data = owner_shop_dict.items()
# # print(save_data)
# current_time = datetime.datetime.now()
# save_data0 = [(list(save_data)[i]) + (current_time,)
#               for i in range(0, len(save_data))]
# save_owner_shops(two_mysql, logger, save_data0)
if __name__ == '__main__':
    pass
