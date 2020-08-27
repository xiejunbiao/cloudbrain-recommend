# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 19:52:16 2020
@author: zhaizhengyuan
计算商家的热度分数模块
用于得到商家的热度排序列表，以区分按销量或者评价排序得到的商家列表
"""
import sys
import numpy as np
import datetime
from hotshop_offline_com.utils.dataBaseHandle import DatabaseHandle
from hotshop_offline_com.utils.config import ModifyConfFile, log_logger
sys.path.append("..")

logger = log_logger()
mcf = ModifyConfFile(logger)
two_mysql = mcf.return_argv()


class HotShopRead(object):
    """
    热门商家读取类：
    计算商家的历史下单数量占比作为商家的销售热度，
    计算商家的历史收藏占比作为商家的收藏热度，
    计算商家的历史好评率和差评率作为商家的评价热度，
    计算商家是否是最新打开作为商家的最新热度，
    sort_shop_list为最后返回的排序结果。
    """
    def __init__(self, two_mysql1, logger1):
        self._logger = logger1
        self.hisense_mysql = two_mysql1['hisense_mysql']
        self.sqyn_mysql = two_mysql1['sqyn_mysql']
        self.read_hisense = DatabaseHandle(self.hisense_mysql, logger1)
        self.read_sqyn = DatabaseHandle(self.sqyn_mysql, logger1)
        self.total_shop_data = None
        self.shop_list = None
        self.shop_sale_score = []
        self.shop_collect_score = []
        self.shop_eval_score = []
        self.new_shop_score = []
        self.sort_shop_list = []
        self.read_shop_info()
        self.sort_shop()

    # 读取所有的店铺列表，同时按照销量降序排序
    def read_shop_info(self):
        mysqlcmd = "SELECT * FROM shop_info WHERE shop_code_type != 4"
        self.total_shop_data = self.read_hisense.read_mysql_multipd(mysqlcmd)
        self.shop_list = self.total_shop_data["shop_code"].tolist()

    # 计算所有店铺的销售热度，将店铺下单数量与总下单数量之比作为该热度评分
    # 为减小店铺的销售热度在总热度评分中的影响，该热度评分增加弱权重0.1
    def count_shop_order(self):
        mysqlcmd = """
            SELECT 
                shop_code, 
                count(shop_code) AS count 
            FROM 
                cb_owner_buy_goods_info 
            GROUP BY 
                shop_code 
            ORDER BY 
                count DESC
        """
        order_data = self.read_sqyn.read_mysql_multipd(mysqlcmd)
        sum1 = order_data["count"].sum()
        self.shop_sale_score = [0 for _ in range(len(self.shop_list))]
        for i in range(len(self.shop_list)):
            try:
                self.shop_sale_score[i] = \
                    order_data[order_data.shop_code ==
                               self.shop_list[i]]["count"].values[0] / sum1 * 0.1
            except IndexError:
                pass
            continue

    # 计算所有店铺的收藏热度，将店铺收藏数量与总收藏数量之比作为该热度评分
    # 为增加店铺的收藏热度在总热度评分中的影响，该热度评分增加强权重20
    def count_shop_collect(self):
        mysqlcmd = """
            SELECT 
                content_code, 
                count(content_code) AS count 
            FROM 
                user_collect_record 
            WHERE 
                collect_type = '02' 
            GROUP BY 
                content_code 
            ORDER BY
                count DESC
        """
        collect_data = self.read_hisense.read_mysql_multipd(mysqlcmd)
        sum1 = collect_data["count"].sum()
        self.shop_collect_score = [0 for _ in range(len(self.shop_list))]
        for i in range(len(self.shop_list)):
            try:
                self.shop_collect_score[i] = \
                    collect_data[collect_data.content_code ==
                                 self.shop_list[i]]["count"].values[0] * 20 / sum1
            except IndexError:
                pass
            continue

    # 计算所有店铺的评价热度，分为两部分：好评率和差评率
    # 好评率和差评率均是指店铺好评、差评数量在总好评、差评数量中所占比例
    # 为区分好评和差评，好评在总热度评分权重为0.1，差评在总热度评分权重为1
    def count_shop_eval(self):
        mysqlcmd = """
            SELECT 
                shop_code,
                count(shop_code) AS count
            FROM 
                goods_spu_eval
            WHERE 
                eval_level = 5
            OR 
                eval_level = 4
            GROUP BY 
                shop_code
            ORDER BY 
                count DESC
        """
        good_eval_data = self.read_hisense.read_mysql_multipd(mysqlcmd)
        mysqlcmd = """
            SELECT 
                shop_code, 
                count(shop_code) AS count 
            FROM 
                goods_spu_eval 
            WHERE 
                eval_level != 5 
            AND 
                eval_level != 4 
            GROUP BY 
                shop_code 
            ORDER BY 
                count DESC
        """
        bad_eval_data = self.read_hisense.read_mysql_multipd(mysqlcmd)
        sum1 = good_eval_data["count"].sum()
        sum2 = bad_eval_data["count"].sum()
        shop_good_eval_score = [0 for _ in range(len(self.shop_list))]
        shop_bad_eval_score = [0 for _ in range(len(self.shop_list))]
        for i in range(len(self.shop_list)):
            try:
                shop_good_eval_score[i] = \
                    good_eval_data[good_eval_data.shop_code ==
                                   self.shop_list[i]]["count"].values[0] / sum1 * 0.1
                shop_bad_eval_score[i] = \
                    bad_eval_data[bad_eval_data.shop_code ==
                                  self.shop_list[i]]["count"].values[0] / sum2
            except IndexError:
                pass
            continue
        self.shop_eval_score = np.subtract(np.array(shop_good_eval_score),
                                           np.array(shop_bad_eval_score)).tolist()

    # 计算所有店铺的最新热度，根据是否是最近10天新开店铺
    # 店铺若为最新开店铺，其最新热度评分为5
    def new_shop(self):
        current_time = datetime.datetime.now()
        begin_time = current_time + datetime.timedelta(days=-7)
        begin_time = begin_time.strftime("%Y-%m-%d")
        end_time = current_time.strftime("%Y-%m-%d")
        mysqlcmd = """
            SELECT 
                shop_code 
            FROM 
                shop_info 
            WHERE 
                created_time 
            BETWEEN 
                '%s' 
            AND 
                '%s'
        """ % (begin_time, end_time)
        new_shop_data = self.read_hisense.read_mysql_multipd(mysqlcmd)
        new_shop_list = new_shop_data["shop_code"].tolist()
        self.new_shop_score = [0.0 for _ in range(len(self.shop_list))]
        for i in range(len(self.shop_list)):
            if self.shop_list[i] in new_shop_list:
                self.new_shop_score[i] = 0.5

    # 计算所有店铺的总热度评分，根据总热度评分进行排序
    # 总热度评分包括销售热度、收藏热度、评价热度和最新热度
    def sort_shop(self):
        self.count_shop_order()
        self.count_shop_collect()
        self.count_shop_eval()
        self.new_shop()

        shop_total_score = [w + x + y + z for w, x, y, z in np.array([
            self.shop_sale_score, self.shop_collect_score,
            self.shop_eval_score, self.new_shop_score]).T]
        shop_score_dict = dict(zip(self.shop_list, shop_total_score))
        shop_score_dict = dict(tuple(
            sorted(shop_score_dict.items(), key=lambda x: x[1], reverse=True)))
        self.sort_shop_list = list(shop_score_dict.keys())


if __name__ == '__main__':
    pass
