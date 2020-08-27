# -*- coding: utf-8 -*-
# @author : zhaizhengyuan
# @datetime : 2020/8/14 11:06
# @file : offline_com_handler.py
# @software : PyCharm

"""
文件说明：

"""
from abc import ABC
import traceback
import tornado.web
import json
import tornado.gen
from concurrent.futures.thread import ThreadPoolExecutor
from tornado.concurrent import run_on_executor
from offline_com.foryou_offline_com.multi_process import start_multi_process


class OfflineComHandler(tornado.web.RequestHandler, ABC):
    """
        多线程
        增加  @run_on_executor
    """

    executor = ThreadPoolExecutor(2)

    def initialize(self, logger, config_result):
        self.__logger = logger
        self.__conf = config_result

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
        message = '初始化数据成功'
        result = ''
        try:
            self.__logger.info("开始初始化数据")  # #用query-作为分隔符
            start_multi_process()
            self.__logger.info('初始化数据成功')

        except Exception as e:
            code = 1
            message = '初始化数据失败'
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