#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time      :2020/8/11 15:24
# @Author    :XieYuHao
 
import json
import math
import random
import tornado.web
import tornado.gen
from concurrent.futures.thread import ThreadPoolExecutor
from utils.tool_kit import ultimate_filter, paginator, get_mysql_conn
from collections import Counter
from tornado.concurrent import run_on_executor


class AlwaysUseHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)

    def initialize(self, logger):
        self.__logger = logger

    async def get(self):
        spu_li_in_stock, spu_li_out_stock = await self.query_all_spu_codes()
        spu_li = spu_li_in_stock + spu_li_out_stock
        if self.get_query_argument('isAll', None):  # 查看全部
            page = int(self.get_query_argument('page'))
            rows = int(self.get_query_argument('rows'))
            data = paginator(page, rows, spu_li)
        else:
            total = len(spu_li)
            # 随机获取三个商品，如不足三个则获取全部
            data = {
                'total': total,
                'spuCodes': self.select_lucky_spu_codes(spu_li_in_stock)
            }
        return await self.finish_response(0, '操作成功', data)

    @run_on_executor
    def query_all_spu_codes(self):
        owner_code = self.get_query_argument('ownerCode')
        area_code = self.get_query_argument('areaCode')
        self.__logger.info('input: {}'.format({'ownerCode': owner_code, 'areaCode': area_code}))
        conn = get_mysql_conn('mysql_2')
        select_sql = '''
            SELECT
                order_info.spu_code, 
                filter.store_status
            FROM
                cb_owner_buy_goods_info AS order_info
                INNER JOIN cb_goods_spu_for_filter AS filter ON filter.spu_code = order_info.spu_code 
                INNER JOIN cb_goods_scope AS scope on order_info.spu_code = scope.spu_code
                INNER JOIN cb_goods_spu_search AS search on filter.spu_code = search.spu_code
            WHERE
                order_info.owner_code = %s 
                AND order_info.rec_area_code = %s
                AND scope.area_code = %s
                AND order_info.paid_time IS NOT NULL
                AND order_info.spu_name NOT REGEXP "链接|差价"
                AND order_info.second_cate NOT IN (100646, 100683)
                AND search.shop_type_code = 1
                AND filter.goods_status = 1
            ORDER BY
                order_info.paid_time
        '''
        select_params = [owner_code, area_code, area_code]
        with conn.cursor() as cur:
            count = cur.execute(select_sql, select_params)
            if not count:
                return [], []
            row_all = cur.fetchall()
        conn.close()
        spu_li_in_stock = [spu_tuple[0] for spu_tuple in row_all if spu_tuple[1] == 1]  # 有库存的spu
        spu_li_out_stock = [spu_tuple[0] for spu_tuple in row_all if spu_tuple[1] != 1]  # 没有库存的spu
        filtered_spu_li_in_stock = ultimate_filter(area_code, self.order_by_count(spu_li_in_stock), ignore_status=True)
        filtered_spu_li_out_stock = ultimate_filter(area_code, self.order_by_count(spu_li_out_stock), ignore_status=True)
        return [filtered_spu_li_in_stock, filtered_spu_li_out_stock]

    # 按购买次数排序，相同的按照购买时间降序排
    @staticmethod
    def order_by_count(origin_li):
        spu_count_map = Counter(origin_li)  # 统计购买次数
        spu_li = list(set(origin_li))
        # 按购买次数降序排序，相同的按购买时间降序
        spu_li.sort(key=lambda spu: (spu_count_map[spu], origin_li.index(spu)), reverse=True)
        return spu_li

    @staticmethod
    def select_lucky_spu_codes(spu_li):
        if len(spu_li) > 3:
            spu_li = random.sample(spu_li, 3)
        return spu_li

    def finish_response(self, res_code, msg, data=None):
        result_dict = {
            'resultCode': res_code,
            'msg': msg,
            'data': data
        }
        self.__logger.info('output: {}'.format(result_dict))
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        return self.finish(json.dumps(result_dict, ensure_ascii=False))

