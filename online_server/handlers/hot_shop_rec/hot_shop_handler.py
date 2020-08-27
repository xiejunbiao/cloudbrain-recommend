import json
import math
from concurrent.futures.thread import ThreadPoolExecutor
import random

import requests
import tornado.web
from pymysql import connect
from redis import StrictRedis
from tornado.concurrent import run_on_executor


class HotShopHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)

    def initialize(self, logger, config_result):
        self.__conf = config_result
        self._logger = logger
        redis_conf = config_result['redis']
        self.redis_conn = StrictRedis(
            host=redis_conf['ip'],
            port=redis_conf['port'],
            db=redis_conf['redis_num'],
            decode_responses=True
        )
        self.pl = self.redis_conn.pipeline()

    async def post(self):
        body_dict = json.loads(self.request.body)
        self._logger.info("input: {}".format(body_dict))
        top_shop_li = body_dict['topShopList']
        owner_code = body_dict['ownerCode']
        self.area_code = body_dict['areaCode']
        self.area_key = self.area_code + '_shop'
        self.personal_key = owner_code + self.area_key
        if int(body_dict['page']) == 1:  # 刷新
            # 刷新redis中用户推荐商铺数据并返回刷新后的全部商铺数据
            shop_li = await self.refresh_rec_shop(owner_code, top_shop_li)
        else:  # 翻页
            shop_li = self.redis_conn.lrange(self.personal_key, 0, -1)
            # 如redis暂无个性化数据，说明redis过期，查询mysql获取推荐数据
            if not shop_li:
                shop_li = await self.refresh_rec_shop(owner_code, top_shop_li)
        await self.finish_response(body_dict, shop_li)
        self.pl.close()
        self.redis_conn.close()

    def finish_response(self, request_body, all_shop_list):
        page = int(request_body['page'])
        rows = int(request_body['rows'])
        start_index = rows * (page - 1)
        result_dict = {
            'resultCode': 0,
            'msg': '操作成功',
            'data': {
                "current": page,  # 当前页
                "pages": math.ceil(len(all_shop_list) / rows),  # 总页数
                "records": all_shop_list[start_index: start_index + rows],
                "size": rows,  # 分页行数
                "total": len(all_shop_list)  # 数据总量
            }
        }
        self._logger.info('output: {}'.format(result_dict))
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        return self.finish(json.dumps(result_dict, ensure_ascii=False))

    # 从mysql获取用户推荐商铺排序用于刷新
    @run_on_executor
    def refresh_rec_shop(self, owner_code, top_shop_list):
        mysql_conn = self.get_mysql_conn()
        select_sql = '''
            SELECT
                  shop_code_list
            FROM
                cb_hotshop_owner_rec_shops 
            WHERE
                owner_code = %s
        '''
        with mysql_conn.cursor() as cur:
            count = cur.execute(select_sql, owner_code)
            if not count:
                # 没有个性化推荐，统一使用冷启动用户推荐结果
                self._logger.info('没有查询到个性化推荐，使用冷启动用户推荐数据')
                shop_list = self.get_strangers_rec(mysql_conn)
            else:
                shop_list = cur.fetchone()[0].split(',')
        mysql_conn.close()
        if top_shop_list:
            shop_list = top_shop_list + shop_list
        shop_list = self.shop_filter(shop_list)
        # 存入redis
        if shop_list:
            try:
                self.pl.delete(self.personal_key)
                self.pl.rpush(self.personal_key, *shop_list)
                self.pl.expire(self.personal_key, 1200)
                self.pl.execute()
            except:
                self._logger.exception('个性化商铺推荐数据写入redis失败')
        return shop_list

    # 冷启动用户的统一商铺推荐结果
    def get_strangers_rec(self, mysql_conn):
        key_for_strangers = '000000'
        # 试图先从redis获取冷启动用户排序
        shop_list = self.redis_conn.lrange(key_for_strangers, 0, -1)
        # redis没获取到需从mysql获取冷启动用户排序并存入redis
        if not shop_list:
            select_sql = '''
                SELECT
                      shop_code_list
                FROM
                    cb_hotshop_owner_rec_shops 
                WHERE
                    owner_code = %s
            '''
            with mysql_conn.cursor() as cur:
                count = cur.execute(select_sql, key_for_strangers)
                if count:
                    shop_list = cur.fetchone()[0].split(',')
                    # 存入redis并设置有效期为12个小时
                    try:
                        self.pl.rpush(key_for_strangers, *shop_list)
                        self.pl.expire(key_for_strangers, 43200)
                        self.pl.execute()
                    except:
                        self._logger.exception('冷启动用户商铺推荐数据写入redis失败')
                # 没查到说明离线计算还没完成，先使用销量排序
                else:
                    shop_list = self.redis_conn.lrange(self.area_key, 0, -1)
        return self.shuffle_top_five(self.shop_filter(shop_list))

    @staticmethod
    def shuffle_top_five(origin_li):
        top_five = origin_li[:5]
        # random.shuffle(top_five)
        top_five.sort(reverse=True)
        return top_five + origin_li[5:]

    # 获取虚拟小区列表
    def get_virtual_areas(self):
        url = '{}/xwj-property-house/house/area/getVirtualAreaCodes'.format(self.__conf['spring_cloud']['host_name'])
        resp_json = json.loads(requests.get(url).text)
        return resp_json.get('data', [])

    # 商铺过滤器（包括小区、商铺状态和去重）
    # 加入虚拟小区的逻辑
    def shop_filter(self, origin_shop_list):
        origin_shop_set = set(origin_shop_list)
        area_shop_set_for_filtering = set(self.redis_conn.lrange(self.area_key, 0, -1))
        if not area_shop_set_for_filtering:  # 没有就说明redis数据丢失, 从mysql获取用于过滤的数据, 并重新写入redis
            area_shop_set_for_filtering = self.get_shop_set_from_mysql()
        # 加上在虚拟小区下的商铺
        virtual_area_li = self.get_virtual_areas()
        for area_code in virtual_area_li:
            shop_set_in_virtual_area = set(self.redis_conn.lrange(area_code + '_shop', 0, -1))
            # self._logger.info('虚拟小区{}下有{}家商铺，商铺列表：{}'.format(
            #     area_code, len(shop_set_in_virtual_area), shop_set_in_virtual_area
            # ))
            area_shop_set_for_filtering |= shop_set_in_virtual_area
        # self._logger.info('加上虚拟小区数据后，小区{}下有{}家商铺，商铺列表：{}'.format(
        #     self.area_key, len(area_shop_set_for_filtering), area_shop_set_for_filtering
        # ))
        filtered_shop_list = list(origin_shop_set.intersection(area_shop_set_for_filtering))
        filtered_shop_list.sort(key=origin_shop_list.index)
        # 把用于过滤的小区商铺数据存在但不在原集合的商铺添加到最后
        return filtered_shop_list + list(area_shop_set_for_filtering - origin_shop_set)

    def get_mysql_conn(self):
        mysql_conf = self.__conf['mysql_2']
        return connect(
            host=mysql_conf['ip'],
            port=int(mysql_conf['port']),
            user=mysql_conf['user'],
            password=mysql_conf['password'],
            database=mysql_conf['db'],
        )

    def get_shop_set_from_mysql(self):
        self._logger.info('redis数据丢失, 使用mysql数据用于过滤并重新导入数据到redis')
        # 判断是否已经在执行导入商铺数据到redis的操作
        # 这个key是在cloudbrain-preprocess-python项目中建立的，导入数据完成后会把这个key删除
        # if not self.redis_conn.exists('loading_shop_flag'):
        #     self.executor.submit(self.load_shop_to_redis)
        mysql_conn = self.get_mysql_conn()
        select_sql = '''
            SELECT
                shop_code_list 
            FROM
                cb_area_shop_list 
            WHERE
                area_code = %s
        '''
        with mysql_conn.cursor() as cur:
            cur.execute(select_sql, self.area_code)
            row_one = cur.fetchone()
        mysql_conn.close()
        shop_li = row_one[0].split(',') if row_one[0] else []
        if shop_li:
            self.redis_conn.rpush(self.area_key, *shop_li)
        return set(shop_li)

    # def load_shop_to_redis(self):
    #     self._logger.info('开始调用preprocess服务加载商铺数据到redis')
    #     url_for_loading = '127.0.0.1:6608/cloudbrain-preprocess-python/preprocess'
    #     requests.get(url_for_loading)
