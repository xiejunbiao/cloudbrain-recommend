import json
import random
import tornado.web
from pymysql import connect
from concurrent.futures.thread import ThreadPoolExecutor

from tornado.concurrent import run_on_executor


class OrderSpuRecHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(5)

    def initialize(self, logger, config_result):
        self.__logger = logger
        self.__conf = config_result

    async def get(self):
        # 查询mysql在订单表中获取满足条件的所有商品（可能有重复），查询条件：用户、小区、上架、有库存
        row_all = await self.query_spu_codes()
        if not row_all:
            return await self.finish_response(0, '操作成功')
        # 去重后，随机获取三个商品，如不足三个则获取全部（排序不定）
        lucky_spu_list = await self.select_lucky_spu_codes(row_all)
        return await self.finish_response(0, '操作成功', lucky_spu_list)

    @run_on_executor
    def query_spu_codes(self):
        owner_code = self.get_query_argument('ownerCode')
        area_code = self.get_query_argument('areaCode')
        self.__logger.info('input: {}'.format({'ownerCode': owner_code, 'areaCode': area_code}))
        mysql_conf = self.__conf['mysql_2']
        conn = connect(
            host=mysql_conf['ip'],
            port=int(mysql_conf['port']),
            user=mysql_conf['user'],
            password=mysql_conf['password'],
            database=mysql_conf['db']
        )
        select_sql = '''
            SELECT
                order_info.spu_code 
            FROM
                cb_owner_buy_goods_info AS order_info
            INNER JOIN 
                cb_goods_spu_for_filter AS spu_filter 
            ON 
                order_info.spu_code = spu_filter.spu_code 
            WHERE
                spu_filter.goods_status = 1 
                AND spu_filter.store_status = 1 
                AND order_info.owner_code = %s
                AND order_info.rec_area_code = %s
        '''
        select_params = [owner_code, area_code]
        with conn.cursor() as cur:
            count = cur.execute(select_sql, select_params)
            if not count:
                return
            row_all = cur.fetchall()
        conn.close()
        return row_all

    @run_on_executor
    def select_lucky_spu_codes(self, row_all):
        all_spu_set = set([spu_tuple[0] for spu_tuple in row_all])
        self.__logger.info('查询到{}个不同商品'.format(len(all_spu_set)))
        k = 3 if len(all_spu_set) >= 3 else len(all_spu_set)
        return random.sample(all_spu_set, k)

    def finish_response(self, res_code, msg, data=None):
        result_dict = {
            'resultCode': res_code,
            'msg': msg,
            'data': data
        }
        self.__logger.info('output: {}'.format(result_dict))
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        return self.finish(json.dumps(result_dict, ensure_ascii=False))
