# -*- coding: utf-8 -*-
"""
Create Time: 2020/7/31 14:04
Author: xiejunbiao
"""
from abc import ABC
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


class NewGoodsUpdateHandler(tornado.web.RequestHandler, GetData, ABC):
    """
        多线程
        增加  @run_on_executor
    """

    executor = ThreadPoolExecutor(20)
    # initialize

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
        self.sql_insert = """
                            INSERT INTO cb_new_goods_rec_results(
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
            self.__logger.info('获取数据完成')
            # 处理数据
            data_all = self.process_data(spu_all)
            self.__logger.info('数据统计数据完成')
            # print(data_all)
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

    def process_data(self, spu_all):

        return self._process(spu_all)

    def _get_spu_list(self):
        # return self._getdata.get_data_spu_all()
        return self.get_data_spu_all_new_goods()

    def get_virtual_areas(self):
        url = '{}/xwj-property-house/house/area/getVirtualAreaCodes'.format(self.__conf['spring_cloud']['host_name'])
        resp_json = json.loads(requests.get(url).text)
        return resp_json.get('data', [])

    def _process(self, spu_all):
        virtual_areas_list = self.get_virtual_areas()
        virtual_code_list = []
        area_all = [each['area_code'] for each in self.get_all_area()]
        # print(area_all)

        # 兼容测试环境转换
        for virtual_code in virtual_areas_list:
            virtual_code_list.append(virtual_code[1:])
            virtual_code_list.append(virtual_code)

        data_list = []
        # 循环每个小区
        for area in area_all:
            if spu_all.empty:
                # print(area_spu_all)
                data_list.append(('000000', area, ''))
                continue
            area_spu_all = spu_all[spu_all['area_code'].isin([area]+virtual_code_list)].drop_duplicates('spu_code')
            if area_spu_all.empty:
                # print(area_spu_all)
                data_list.append(('000000', area, ''))
            else:
                # print(area_spu_all)
                tmp_group = area_spu_all.groupby('first_cate')
                cate_list = list(tmp_group.size().index)
                m_list = []
                dict_data = {}
                # 循环每个类目
                for each_cate in cate_list:
                    data_f = tmp_group.get_group(each_cate).sort_values('created_time', ascending=False)
                    m_list.append(len(data_f.values.tolist()))
                    # print(data_f['spu_code'].values.tolist())
                    dict_data[each_cate] = data_f['spu_code'].values.tolist()
                    # print(dict_data[each_cate])
                data_f_list = self.resort_by_multiple(dict_data, m_list)
                # data_f_ = list(set(data_f_list))
                # data_f_.sort(key=data_f_list.index)
                # print(data_f_list)
                data_list.append(('000000', area, ','.join(data_f_list)))
                # print(data_list)

        return data_list

    @staticmethod
    def resort_by_multiple(a_dict, b_list):
        """
        方法用于按蛇形顺序依次从a_dict取出元素重新排序
        输入：字典a_dict的键值为list,b_list中每个元素为该list长度
        输出：重新排序后的list
        功能：用于将a_dict转化成一个蛇形排序的list
        """
        all_sorted_list = []
        for i in range(max(b_list)):
            for cate in a_dict.keys():
                try:
                    all_sorted_list.append(a_dict[cate][i])
                except IndexError:
                    continue
        return all_sorted_list

    def _data_insert(self, data_all):
        _mysql_conn_sqyn = get_mysql_conn('mysql_2')
        _cursor_mysql_sqyn = _mysql_conn_sqyn.cursor(cursor=DictCursor)
        _cursor_mysql_sqyn.executemany(self.sql_insert, data_all)
        _mysql_conn_sqyn.commit()
        _cursor_mysql_sqyn.close()
        _mysql_conn_sqyn.close()


if __name__ == '__main__':
    # pass
    from online_server.main import getLogger
    from online_server.utils.configInit import config_dict
    nguh = NewGoodsUpdateHandler(getLogger('text'), config_dict)
    nguh.update_data()
    # print(dhsh.get_virtual_areas())
