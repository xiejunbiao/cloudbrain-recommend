# -*- coding: utf-8 -*-
"""
Create Time: 2020/8/12 8:42
Author: xiejunbiao
"""
import json
import random
import traceback
from abc import ABC
import tornado.web
import tornado.gen
from concurrent.futures.thread import ThreadPoolExecutor
from tornado.concurrent import run_on_executor
from online_server.utils.tool_kit import ultimate_filter, paginator
from online_server.handlers.new_goods_and_dis_hot_sale.getData import GetDataFromDatabase as GetData
from online_server.utils.tool_kit import get_mysql_conn


class DisHotSaleAndNewGoodsHomeHandler(tornado.web.RequestHandler, GetData, ABC):
    """
        多线程
        增加  @run_on_executor
    """

    executor = ThreadPoolExecutor(20)

    # initialize
    def initialize(self, logger, config_result):
        GetData.__init__(self)
        self.__conf = config_result
        self.__logger = logger
        self._mysql_conn_sqyn = get_mysql_conn('mysql_2')
        self._cursor_mysql_sqyn = self._mysql_conn_sqyn.cursor()

    @tornado.gen.coroutine
    def get(self):

        """get请求"""
        area_code = self.get_argument('areaCode')
        page_dict = yield self.get_query_answer(area_code)
        page_json = json.dumps(page_dict, ensure_ascii=False)
        self.write(page_json)

    @run_on_executor
    def get_query_answer(self, area_code):

        """把异常写进日志"""
        code = 0
        message = '调用成功'
        result = ''
        try:

            self.__logger.info("parameter area_code-{}".format(area_code))  # #用query-作为分隔符

            result = self.get_data(area_code)
            self.__logger.info(
                    '数据获取成功:一共{}条数据'.format(result))
        except Exception as e:
            code = 1
            message = '调用失败'
            self.__logger.info("error:")
            self.__logger.info(e)
            self.__logger.info("traceback My:")
            self.__logger.info(traceback.format_exc())  # #返回异常信息的字符串，可以用来把信息记录到log里
        result_json = {
            'resultCode': code,
            'msg': message,
            'data': result
        }
        return result_json

    def get_data(self, area_code):
        """

        """

        return {'discount_hot_sale': self.__get_data_discount_hot_sale(area_code),
                'new_goods': self.__get_data_new_goods(area_code)}

    def __get_data_discount_hot_sale(self, area_code):
        select_sql_result = """SELECT spu_code_list FROM `cb_discount_goods_rec_results`  
                 WHERE area_code='{}'
                 """.format(area_code)

        _sql_select_dis_hot_goods = """SELECT	
                                                a.spu_code
                                            FROM        cb_goods_spu_for_filter AS a
                                            JOIN    cb_goods_scope AS b ON a.spu_code=b.spu_code
                                            JOIN cb_goods_spu_search c ON a.spu_code=c.spu_code 
                                        WHERE a.price_line > a.sale_price
                                                AND c.shop_type_code = 1
                                                AND a.spu_name NOT REGEXP "链接|差价"
                                                AND a.second_cate NOT IN (100646, 100683)
                                                AND b.area_code IN ({})
                                                GROUP BY a.spu_code
                                                order by a.sale_month_count DESC
                           """.format("'%s','2020032600001','A2020032600001'" % area_code)

        count = self._cursor_mysql_sqyn.execute(select_sql_result)
        if not count:
            self.__logger.info('优惠热卖数据查询为空，进行二次查询')
            self._cursor_mysql_sqyn.execute(_sql_select_dis_hot_goods)
            data = self._cursor_mysql_sqyn.fetchall()
            # print(data)
            data_list_t = [li_data[0] for li_data in data]
            # print(data_list_t[:10])
            self.__logger.info('DIS-HOME-过滤前---' + str(len(data_list_t)))
            data_list_t = ultimate_filter(area_code, data_list_t)
            self.__logger.info('DIS-HOME-过滤后---' + str(len(data_list_t)))
            if len(data_list_t) < 2:
                return data_list_t[:2]
            return random.sample(data_list_t[:50], 2)

        data_list = self._cursor_mysql_sqyn.fetchone()[0].split(',')
        # print(data_list)
        # 过滤之后选择前50个
        self.__logger.info('DIS-HOME-过滤前---' + str(len(data_list)))
        data_list = ultimate_filter(area_code, data_list)
        self.__logger.info('DIS-HOME-过滤后---' + str(len(data_list)))
        if len(data_list) < 2:
            self.__logger.info('优惠热卖数据总数不足两个，进行二次查询')
            self._cursor_mysql_sqyn.execute(_sql_select_dis_hot_goods)
            data = self._cursor_mysql_sqyn.fetchall()
            # print(data)
            data_list_t = [li_data[0] for li_data in data]
            # print(data_list_t[:10])
            self.__logger.info('DIS-HOME-过滤前---' + str(len(data_list_t)))
            data_list_t = ultimate_filter(area_code, data_list_t)
            self.__logger.info('DIS-HOME-过滤后---' + str(len(data_list_t)))
            if len(data_list_t) < 2:
                return data_list_t[:2]
            return random.sample(data_list_t[:50], 2)

        if len(data_list) > 50:
            data_list = data_list[:50]
        return random.sample(data_list, 2)  # 从50个中随机选择2个

    def __get_data_new_goods(self, area_code):
        select_sql = """SELECT spu_code_list FROM cb_new_goods_rec_results
                 WHERE area_code='{}'
                 """.format(area_code)
        _sql_select_spu_code_new_goods_area = """SELECT   	
                                                    a.spu_code
                                                FROM        cb_goods_spu_for_filter AS a
                                                JOIN    cb_goods_scope AS b ON a.spu_code=b.spu_code
                                                JOIN cb_goods_spu_search c ON a.spu_code=c.spu_code 
                                            WHERE   a.sale_price > 10
                                                    AND c.shop_type_code = 1
                                                    AND a.spu_name NOT REGEXP "链接|差价"
                                                    AND a.second_cate NOT IN (100646, 100683)
                                                    AND b.area_code IN ({})
                                                    GROUP BY a.spu_code
                                                    ORDER BY a.created_time DESC
                                                """.format("'%s','2020032600001','A2020032600001'" % area_code)
        count = self._cursor_mysql_sqyn.execute(select_sql)
        if not count:
            self.__logger.info('新品好货数据查询为空，进行二次查询')
            self._cursor_mysql_sqyn.execute(_sql_select_spu_code_new_goods_area)
            data = self._cursor_mysql_sqyn.fetchall()
            # print(data)
            data_list_t = [li_data[0] for li_data in data]
            # print(data_list_t[:10])
            self.__logger.info('NEW-HOME-过滤前---' + str(len(data_list_t)))
            data_list_t = ultimate_filter(area_code, data_list_t)
            self.__logger.info('NEW-HOME-过滤后---' + str(len(data_list_t)))
            if len(data_list_t) < 2:
                return data_list_t[:2]
            return random.sample(data_list_t[:50], 2)
        data_list = self._cursor_mysql_sqyn.fetchone()[0].split(',')
        self.__logger.info('NEW-HOME-过滤前---' + str(len(data_list)))
        data_list = ultimate_filter(area_code, data_list)
        self.__logger.info('NEW-HOME-过滤后---' + str(len(data_list)))
        if len(data_list) < 2:
            self.__logger.info('新品好货数据总数不足两个，进行二次查询')
            self._cursor_mysql_sqyn.execute(_sql_select_spu_code_new_goods_area)
            data = self._cursor_mysql_sqyn.fetchall()
            # print(data)
            data_list_t = [li_data[0] for li_data in data]
            # print(data_list_t[:10])
            self.__logger.info('NEW-HOME-过滤前---' + str(len(data_list_t)))
            data_list_t = ultimate_filter(area_code, data_list_t)
            self.__logger.info('NEW-HOME-过滤后---' + str(len(data_list_t)))
            if len(data_list_t) < 2:
                return data_list_t[:2]
            return random.sample(data_list_t[:50], 2)
        if len(data_list) > 50:
            data_list = data_list[:50]
        return random.sample(data_list, 2)


class DisHotSaleAndNewGoodsAllHandler(tornado.web.RequestHandler, GetData, ABC):
    """
    多线程
    增加  @run_on_executor
    """

    executor = ThreadPoolExecutor(20)

    def initialize(self, logger, config_result):
        GetData.__init__(self)
        self.__conf = config_result
        self.__logger = logger
        self._mysql_conn_sqyn = get_mysql_conn('mysql_2')
        self._cursor_mysql_sqyn = self._mysql_conn_sqyn.cursor()

    # def initialize(self, logger, conf):

    @tornado.gen.coroutine
    def get(self):

        """get请求"""
        tag_id = self.get_argument('tag_id')
        page = int(self.get_argument('page'))
        row = int(self.get_argument('row'))
        area_code = self.get_argument('areaCode')
        page_dict = yield self.get_query_answer(tag_id, area_code, page, row)
        page_json = json.dumps(page_dict, ensure_ascii=False)
        self.write(page_json)

    @run_on_executor
    def get_query_answer(self, tag_id, area_code, page, row):

        """把异常写进日志"""
        code = 0
        message = '调用成功'
        result = ''
        try:
            self.__logger.info("parameter tag_id-{}".format(tag_id))  # #用query-作为分隔符
            self.__logger.info("parameter area_code-{}".format(area_code))  # #用query-作为分隔符
            self.__logger.info("parameter page-{}".format(page))  # #用query-作为分隔符
            self.__logger.info("parameter row-{}".format(row))  # #用query-作为分隔符
            result = self.get_data(tag_id, area_code, page, row)
            self.__logger.info('数据获取成功:{}'.format(result))

        except Exception as e:
            code = 1
            message = '调用失败'
            self.__logger.info("error:")
            self.__logger.info(e)
            self.__logger.info("traceback My:")
            self.__logger.info(traceback.format_exc())  # #返回异常信息的字符串，可以用来把信息记录到log里
        result_json = {
            'resultCode': code,
            'msg': message,
            'data': result
        }
        return result_json

    def get_data(self, tag_id, area_code, page, row):
        if tag_id == 'discount_hot_sale':
            return self.__get_data_discount_hot_sale_all(area_code, page, row)
        elif tag_id == 'new_goods':
            return self.__get_data_new_goods_all(area_code, page, row)
        else:
            return '你输入的参数tag_id={}不正确'.format(tag_id)

    def __get_data_discount_hot_sale_all(self, area_code, page, row):
        select_sql = """SELECT spu_code_list FROM `cb_discount_goods_rec_results`  
                 WHERE area_code='{}'
                 """.format(area_code)

        _sql_select_dis_hot_goods = """SELECT	
                                                a.spu_code
                                            FROM        cb_goods_spu_for_filter AS a
                                            JOIN    cb_goods_scope AS b ON a.spu_code=b.spu_code
                                            JOIN cb_goods_spu_search c ON a.spu_code=c.spu_code 
                                        WHERE a.price_line > a.sale_price
                                                AND c.shop_type_code = 1
                                                AND a.spu_name NOT REGEXP "链接|差价"
                                                AND a.second_cate NOT IN (100646, 100683)
                                                AND b.area_code IN ({})
                                                GROUP BY a.spu_code
                                                ORDER BY a.sale_month_count DESC
                                   """.format("'%s','2020032600001','A2020032600001'" % area_code)

        count = self._cursor_mysql_sqyn.execute(select_sql)
        if not count:
            self.__logger.info('优惠热卖数据查询为空，进行二次查询')
            self._cursor_mysql_sqyn.execute(_sql_select_dis_hot_goods)
            data = self._cursor_mysql_sqyn.fetchall()
            # print(data[])
            data_list_t = [li_data[0] for li_data in data]
            # print(data_list_t[:10])
            self.__logger.info('DIS-ALL-过滤前---' + str(len(data_list_t)))
            data_list_t = ultimate_filter(area_code, data_list_t)
            self.__logger.info('DIS-ALL-过滤后---' + str(len(data_list_t)))
            return paginator(page, row, data_list_t[:50])

        data_list = self._cursor_mysql_sqyn.fetchone()[0].split(',')

        # print(data_list)

        # 过滤之后选择前50个
        self.__logger.info('DIS-ALL-过滤前---' + str(len(data_list)))
        data_list = ultimate_filter(area_code, data_list)
        # print(data_list[:10])
        self.__logger.info('DIS-ALL-过滤后---' + str(len(data_list)))
        # print(data_list)
        if len(data_list) < 2:
            self.__logger.info('优惠热卖数据总数不足两个，进行二次查询')
            self._cursor_mysql_sqyn.execute(_sql_select_dis_hot_goods)
            data = self._cursor_mysql_sqyn.fetchall()
            # print(data)
            data_list_t = [li_data[0] for li_data in data]
            # print(data_list_t[:10])
            self.__logger.info('NDIS-ALL-过滤前---' + str(len(data_list_t)))
            data_list_t = ultimate_filter(area_code, data_list_t)
            self.__logger.info('NDIS-ALL-过滤后---' + str(len(data_list_t)))
            return paginator(page, row, data_list_t[:50])
        if len(data_list) > 50:
            data_list = data_list[:50]
        return paginator(page, row, data_list)

    def __get_data_new_goods_all(self, area_code, page, row):
        # print(area_code)
        select_sql = """SELECT spu_code_list FROM cb_new_goods_rec_results
                 WHERE area_code='{}'
                 """.format(area_code)
        _sql_select_spu_code_new_goods_area = """SELECT   	
                                                    a.spu_code
                                                FROM        cb_goods_spu_for_filter AS a
                                                JOIN    cb_goods_scope AS b ON a.spu_code=b.spu_code
                                                JOIN cb_goods_spu_search c ON a.spu_code=c.spu_code 
                                            WHERE   a.sale_price > 10
                                                    AND c.shop_type_code = 1
                                                AND a.spu_name NOT REGEXP "链接|差价"
                                                AND a.second_cate NOT IN (100646, 100683)
                                                AND b.area_code IN ({})
                                                GROUP BY a.spu_code
                                                ORDER BY a.created_time DESC
                                   """.format("'%s','2020032600001','A2020032600001'" % area_code)
        count = self._cursor_mysql_sqyn.execute(select_sql)

        if not count:
            self.__logger.info('新品好货数据查询为空，进行二次查询')
            self._cursor_mysql_sqyn.execute(_sql_select_spu_code_new_goods_area)
            data = self._cursor_mysql_sqyn.fetchall()
            data_list_t = [li_data[0] for li_data in data]
            self.__logger.info('NEW-ALL-过滤前---' + str(len(data_list_t)))
            data_list_t = ultimate_filter(area_code, data_list_t)
            self.__logger.info('NEW-ALL-过滤后---' + str(len(data_list_t)))
            return paginator(page, row, data_list_t[:50])

        data_list = self._cursor_mysql_sqyn.fetchone()[0].split(',')
        # print(data_list)
        self.__logger.info('NEW-ALL-过滤前---' + str(len(data_list)))
        data_list = ultimate_filter(area_code, data_list)
        self.__logger.info('NEW-ALL-过滤后---' + str(len(data_list)))
        if len(data_list) < 2:
            self.__logger.info('新品好货数据总数不足两个，进行二次查询')
            self._cursor_mysql_sqyn.execute(_sql_select_spu_code_new_goods_area)
            data = self._cursor_mysql_sqyn.fetchall()
            data_list_t = [li_data[0] for li_data in data]
            # print(data_list_t[:10])
            self.__logger.info('NEW-ALL-过滤前---' + str(len(data_list_t)))
            data_list_t = ultimate_filter(area_code, data_list_t)
            self.__logger.info('NEW-ALL-过滤后---' + str(len(data_list_t)))
            return paginator(page, row, data_list_t[:50])

        if len(data_list) > 50:
            data_list = data_list[:50]

        return paginator(page, row, data_list)


if __name__ == '__main__':
    pass
    from online_server.main import getLogger
    from online_server.utils.configInit import config_dict
    #
    # tag_id = 'discount_hot_sale'
    tag_id1 = 'new_goods'
    # area_code = 'A2018012300015'
    area_code1 = '056'
    page1 = 1
    row1 = 10

    dhsuh = DisHotSaleAndNewGoodsHomeHandler(getLogger('text'), config_dict)
    dhsuh1 = DisHotSaleAndNewGoodsAllHandler(getLogger('text'), config_dict)
    drte = dhsuh.get_data(area_code1)
    drte1 = dhsuh1.get_data(tag_id1, area_code1, page1, row1)
    print(drte1)
