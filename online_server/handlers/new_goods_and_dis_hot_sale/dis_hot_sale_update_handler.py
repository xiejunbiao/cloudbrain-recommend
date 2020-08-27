# -*- coding: utf-8 -*-
"""
Create Time: 2020/7/31 14:04
Author: xiejunbiao
"""
from abc import ABC
import operator
import pandas as pd
import traceback
import json
import requests
import tornado.web
import tornado.gen
from pymysql.cursors import DictCursor
from concurrent.futures.thread import ThreadPoolExecutor
from online_server.handlers.new_goods_and_dis_hot_sale.getData import GetDataFromDatabase as GetData
from tornado.concurrent import run_on_executor
from online_server.utils.tool_kit import get_mysql_conn


class DisHotSaleUpdateHandler(tornado.web.RequestHandler, GetData, ABC):
    """
        多线程
        增加  @run_on_executor
    """

    executor = ThreadPoolExecutor(20)

    def initialize(self, logger, config_result):
        GetData.__init__(self, service_code_tuple=(100646, 100683), txt_list=['链接', '差价'], dict_=True)
        self.__logger = logger
        self.__conf = config_result
        # self._mysql_sqyn = self.__conf['mysql_2']
        # self._mysql_hisense = self.__conf['mysql_1']
        self._mysql_conn_sqyn = get_mysql_conn('mysql_2')
        self._cursor_mysql_sqyn = self._mysql_conn_sqyn.cursor(cursor=DictCursor)
        self._mysql_conn_hisense = get_mysql_conn('mysql_1')
        self._cursor_mysql_hisense = self._mysql_conn_sqyn.cursor(cursor=DictCursor)
        # self._getdata = GetData(service_code_tuple=(100646, 100683), txt_list=['链接', '差价'], dict_=True)
        self.sql_insert = """
                            INSERT INTO cb_discount_goods_rec_results (
                                owner_code,
                                area_code,
                                spu_code_list
                                )
                            VALUES ({})
                            ON DUPLICATE KEY update
                            owner_code = VALUES(owner_code),
                            area_code = VALUES(area_code),
                            spu_code_list = VALUES(spu_code_list)
                            """.format('%s' + ', %s' * 2)
    # def initialize(self, ):

    @tornado.gen.coroutine
    def get(self):

        """get请求"""

        page_dict = yield self.get_query_answer()
        page_json = json.dumps(page_dict, ensure_ascii=False)
        self.write(page_json)

    @run_on_executor
    def get_query_answer(self, ):

        """把异常写进日志"""
        code = 0
        message = '更新调用成功'
        result = ''
        try:
            self.__logger.info("开始更新数据")  # #用query-作为分隔符
            result = self.update_data()
            self.__logger.info('数据更新成功:{}'.format(result))

        except Exception as e:
            code = 1
            message = '更新调用失败'
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

    def update_data(self):
        """
        如果type_args==01返回随机首页商品
        """
        try:
            # 获得数据
            spu_all = pd.DataFrame(self._get_spu_list())
            coupon_data = pd.DataFrame(self._get_coupon_shop())
            promotion_spu = pd.DataFrame(self._get_promotion_spu())
            # price_line_spu = pd.DataFrame(self._get_price_line_spu())
            self.__logger.info('获取数据完成')

            # 处理数据
            data_all = self.process_data(spu_all, coupon_data, promotion_spu)
            self.__logger.info('数据统计数据完成')

            # 存储数据
            self.update_spu_list(data_all)
            self.__logger.info('数据更新到数据库完成')
            return '更新成功'
        except Exception as e:
            # print(e)
            self.__logger.error(traceback.format_exc())
            return '更新异常'

    def update_spu_list(self, _data):
        self._data_insert(_data)

    def process_data(self, spu_all, coupon_data, promotion_spu):

        return self._process(spu_all, coupon_data, promotion_spu)

    def _get_spu_list(self):
        # return self._getdata.get_data_spu_all()
        return self.get_data_spu_all()

    def _get_coupon_shop(self):
        # return self._getdata.get_data_coupon_shop()
        return self.get_data_coupon_shop()

    def _get_promotion_spu(self):
        # return self._getdata.get_data_promotion()
        return self.get_data_promotion()

    def _get_price_line_spu(self):
        # return self._getdata.get_data_spu_price_line()
        return self.get_data_spu_price_line()

    def get_virtual_areas(self):
        url = '{}/xwj-property-house/house/area/getVirtualAreaCodes'.format(self.__conf['spring_cloud']['host_name'])
        resp_json = json.loads(requests.get(url).text)
        return resp_json.get('data', [])

    def _process(self, spu_all, coupon_data, promotion_spu):
        virtual_areas_list = self.get_virtual_areas()
        # print(virtual_areas_list)
        virtual_code_list = []
        area_all = [each['area_code'] for each in self.get_all_area()]
        # print(area_all)

        # 兼容测试环境转换
        for virtual_code in virtual_areas_list:
            virtual_code_list.append(virtual_code[1:])
            virtual_code_list.append(virtual_code)
        self.__logger.info('虚拟小区:{}'.format(virtual_code_list))
        # 虚拟小区数据
        data_vietual = spu_all[spu_all['area_code'].isin(virtual_code_list)]

        group_virtual_area = data_vietual.groupby('area_code')
        area_all_virtual = list(group_virtual_area.size().index)

        # 非虚拟小区数据
        group_not_virtual_area = spu_all[~spu_all['area_code'].isin(virtual_code_list)].groupby('area_code')
        area_all_not_virtual = list(group_not_virtual_area.size().index)

        # 优惠券数据分组
        if not coupon_data.empty:
            coupon_data_02 = list(coupon_data[coupon_data['coupon_type'].isin(['02'])]
                                  .groupby(['area_code', 'shop_code']).size().index)
            coupon_data_03 = list(coupon_data.loc[coupon_data['coupon_type'].isin(['03'])]
                                  .groupby(['area_code', 'shop_code']).size().index)
        else:
            coupon_data_02 = []
            coupon_data_03 = []

        # 促销活动
        if not promotion_spu.empty:
            promotion_data = list(promotion_spu.groupby(['area_code', 'spu_code']).size().index)
        else:
            promotion_data = []

        # 降价（划线价高于销售价）
        # if not price_line_spu.empty:
        #     discount_data = list(price_line_spu.groupby(['area_code', 'spu_code']).size().index)
        # else:
        #     discount_data = []
        # 统计非虚拟小区数据
        self.__logger.info('开始统计非虚拟小区数据')
        data_not_virtual_area = self._process_fun(group_not_virtual_area,
                                                  promotion_data=promotion_data,
                                                  # discount_data=discount_data,
                                                  coupon_data_02=coupon_data_02,
                                                  coupon_data_03=coupon_data_03)
        self.__logger.info('非虚拟小区数据统计完成')
        # 虚拟小区数据
        self.__logger.info('开始统计虚拟小区数据')
        data_virtual_area = self._process_fun(group_virtual_area,
                                              promotion_data=promotion_data,
                                              # discount_data=discount_data,
                                              coupon_data_02=coupon_data_02,
                                              coupon_data_03=coupon_data_03)
        self.__logger.info('虚拟小区数据统计完成')

        # 将虚拟小区数据和非虚拟小区数据进行合并
        data_merge = pd.concat([data_not_virtual_area, data_virtual_area])
        data_all = data_merge.reset_index(drop=True)
        data_list = []
        # 按照小区抽取并进行排序
        self.__logger.info('小区数据格式转化')
        for area in area_all:
            data_tmp_a = data_all[data_all['area_code'].isin(area_all_virtual + [area])].drop_duplicates('spu_code')
            if data_tmp_a.empty:
                data_list.append(('000000', area, ''))
            else:
                data_area = data_tmp_a.sort_values(by=['sale_count', 'discount_cate_num', 'spu_code'],
                                                   ascending=[False, False, False])
                data_f_list = data_area['spu_code'].values.tolist()
                # data_f_ = list(set(data_f_list))
                # data_f_.sort(key=data_f_list.index)
                self.__logger.info("{}:--共有数据{}条".format(area, len(data_f_list)))
                data_list.append(('000000', area, ','.join(data_f_list)))

        return data_list

    def _data_insert(self, data_all):
        _mysql_conn_sqyn = get_mysql_conn('mysql_2')
        _cursor_mysql_sqyn = _mysql_conn_sqyn.cursor(cursor=DictCursor)
        _cursor_mysql_sqyn.executemany(self.sql_insert, data_all)
        _mysql_conn_sqyn.commit()
        _cursor_mysql_sqyn.close()
        _mysql_conn_sqyn.close()

    @staticmethod
    def _process_fun(group_, **kwargs):
        promotion_data = kwargs['promotion_data']
        # discount_data = kwargs['discount_data']
        coupon_data_02 = kwargs['coupon_data_02']
        coupon_data_03 = kwargs['coupon_data_03']
        area_all = list(group_.size().index)
        data_v = []
        for each_area in area_all:

            data_1 = group_.get_group(each_area)
            group_all_shop = data_1.groupby('shop_code')
            shop_all = list(group_all_shop.size().index)
            for each_shop in shop_all:
                data_2 = group_all_shop.get_group(each_shop)
                spu_code_tmp = list(data_2['spu_code'].values)
                for each_spu_2 in spu_code_tmp:
                    tmp_dict = {'area_code': each_area,
                                'spu_code': each_spu_2,
                                'discount_cate_num': 0,
                                'sale_count': int(data_2[data_2['spu_code'].isin([each_spu_2])]['sale_month_count'])}

                    # 统计促销
                    if operator.contains(promotion_data, (each_area, each_spu_2)):
                        tmp_dict['discount_cate_num'] += 1

                    # 统计是否降价
                    # if operator.contains(discount_data, (each_area, each_spu_2)):
                    #     tmp_dict['discount_cate_num'] += 1

                    # 统计店铺优惠券
                    if operator.contains(coupon_data_02, (each_area, each_shop)):
                        tmp_dict['discount_cate_num'] += 1

                    # 统计平台-店铺优惠券
                    if operator.contains(coupon_data_03, (each_area, each_shop)):
                        tmp_dict['discount_cate_num'] += 1
                    # print(tmp_dict)
                    data_v.append(tmp_dict)
        return pd.DataFrame(data_v)


if __name__ == '__main__':
    from online_server.main import getLogger
    from online_server.utils.configInit import config_dict
    dhsuh = DisHotSaleUpdateHandler(getLogger('text'), config_dict)
    dhsuh.update_data()
    # print(dhsh.get_virtual_areas())
