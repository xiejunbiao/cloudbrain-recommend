#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time      :2020/8/11 17:11
# @Author    :XieYuHao
 
import json
import requests
import tornado.web
from concurrent.futures.thread import ThreadPoolExecutor

from utils.load_data_task import load_area_spu_to_redis
from utils.tool_kit import ultimate_filter, get_redis_conn, get_mysql_conn, remove_duplication
from redis import ResponseError
from tornado.concurrent import run_on_executor
from utils.configInit import config_dict


class GuessYouLikeHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)

    def initialize(self, logger):
        self._logger = logger
        self.r_conn = get_redis_conn()
        self.pl = self.r_conn.pipeline()

    async def post(self):
        spu_li = await self.get_spu_codes()
        await self.finish_response(spu_li)

    @run_on_executor
    def get_spu_codes(self):
        body_dict = json.loads(self.request.body)
        self._logger.info("input: {}".format(body_dict))
        self.owner_code = body_dict['ownerCode']
        self.area_code = body_dict['areaCode']
        self.tab_code = body_dict['tabCode']
        self.page = int(body_dict['page'])
        self.rows = int(body_dict['rows'])
        self.individual_key = '{}_{}_{}'.format(
        self.owner_code, self.area_code, self.tab_code)  # redis和mysql都用这个检索
        self.has_nex_record_key = self.individual_key + '_1'
        self.is_last_page = False
        # 优先从redis获取
        if self.r_conn.exists(self.individual_key):
            spu_li = self.get_individual_spu_codes_from_redis()
        else:
            spu_li = self.get_individual_spu_codes_from_mysql()
            if self.page == 1:  # 新用户或是redis数据过期
                if len(spu_li) <= self.rows:  # 过滤完数量不足则再次从Mysql获取知道满足数量或是达到最大请求次数5
                    count = 0
                    while len(spu_li) <= self.rows and count < 5:
                        spu_li.extend(self.get_individual_spu_codes_from_mysql())
                        count += 1
                        self._logger.info(f'拼接第{count}次')
                    spu_li = remove_duplication(spu_li)
                    if len(spu_li) <= self.rows:  # 多次拼接还是数量不足说明该小区下只有这些spu
                        self.is_last_page = True
            else:  # 翻页时redis没有缓存数据说明过期，根据请求页数截取返回
                start_index = (self.page - 1) * self.rows
                spu_li = spu_li[start_index:]
        if not spu_li or self.is_last_page:
            self.is_last_page = True
            return spu_li
        try:
            self.pl.rpush(self.individual_key, *spu_li[self.rows:])
            self.pl.expire(self.individual_key, 1800)  # 半小时有效期
            self.pl.execute()
        except ResponseError:
            self._logger.exception('剩余商品写入redis异常')
        return spu_li[:self.rows]

    def on_finish(self) -> None:
        self.pl.close()
        self.r_conn.close()

    def finish_response(self, spu_li):
        result_dict = {
            'resultCode': 0,
            'msg': '操作成功',
            'data': {
                "spuCodes": spu_li,
                "isLastPage": self.is_last_page
            }
        }
        self._logger.info('output: {}'.format(result_dict))
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        return self.finish(json.dumps(result_dict, ensure_ascii=False))

    def get_individual_spu_codes_from_redis(self) -> list:
        # self._logger.info('使用redis数据')
        # 把redis所存的全部拿出来过滤
        self.pl.lrange(self.individual_key, 0, -1)
        self.pl.delete(self.individual_key)
        filtered_spu_li = self.filter(self.pl.execute()[0])
        if len(filtered_spu_li) <= self.rows:  # 过滤后数量小于或等于请求个数，刷新操作总会从mysql获取数据拼接到后面
            if self.page == 1:  # 刷新
                filtered_spu_li.extend(self.get_individual_spu_codes_from_mysql())
                filtered_spu_li = remove_duplication(filtered_spu_li)  # 去重
            else:  # 翻页  需要判断mysql是否有下一条数据, 没有就是最后一页
                if self.r_conn.get(self.has_nex_record_key) == '1':
                    filtered_spu_li.extend(self.get_individual_spu_codes_from_mysql())
                else:
                    self.is_last_page = True
        return filtered_spu_li

    def get_individual_spu_codes_from_mysql(self):
        redis_index_key = self.individual_key + '_index'  # 标志用户看到mysql中的第几条数据了
        redis_index = self.r_conn.get(redis_index_key) if self.r_conn.exists(redis_index_key) else 1
        self.mysql_conn = get_mysql_conn('mysql_2')
        select_rec_sql = '''
            SELECT
                spu_code_list,
                has_next_record
            FROM
                cb_guesslike_owner_rec_goods
            WHERE
                owner_area_tab_code = %s 
                AND redis_index = %s
        '''
        select_rec_params = [self.individual_key, redis_index]
        with self.mysql_conn.cursor() as cur:
            count = cur.execute(select_rec_sql, select_rec_params)
            if not count:  # 没有个性化推荐使用冷启动用户通用
                self._logger.info('没有个性化数据，使用冷启动用户通用')
                return self.get_stranger_spu_codes()
            spu_codes, has_next_record = cur.fetchone()
        self.mysql_conn.close()
        # 从mysql读取一条记录后，redis中存储的index要加1, 如没有下一条设为1
        redis_index = int(redis_index) + 1 if has_next_record else 1
        # 记录用户读取的位置到redis, 设置同样的有效期12小时
        try:
            self.pl.setex(self.has_nex_record_key, 43200, has_next_record)
            self.pl.setex(redis_index_key, 43200, redis_index)
            self.pl.execute()
        except:
            self._logger.exception('用户读取mysql的标识写入redis失败')
        else:
            self._logger.info('用户读取mysql的标识写入redis成功')
        # 这需要过滤秒杀商品所以需要用到终极过滤器
        return ultimate_filter(self.area_code, spu_codes.split(','))

    # 获取链接|差价商品
    def get_link_spu_codes(self):
        select_sql = '''
            SELECT
                filter.spu_code
            FROM
                cb_goods_spu_for_filter AS filter
                INNER JOIN cb_goods_scope AS scope ON filter.spu_code = scope.spu_code 
            WHERE
                scope.area_code = %s
                AND filter.spu_name REGEXP "链接|差价"
        '''
        select_params = [self.area_code]
        with self.mysql_conn.cursor() as cur:
            count = cur.execute(select_sql, select_params)
            if not count:
                return []
            row_all = cur.fetchall()
        return [spu_tuple[0] for spu_tuple in row_all]

    # 获取冷启动用户通用数据
    def get_stranger_spu_codes(self):
        # link_spu_li = self.get_link_spu_codes()
        stranger_key = '000_' + self.tab_code
        # 先从redis获取
        spu_li = self.r_conn.lrange(stranger_key, 0, -1)
        if not spu_li:
            spu_li = self.get_stranger_spu_codes_from_mysql(stranger_key)
        return self.filter(spu_li)

    def get_stranger_spu_codes_from_mysql(self, stranger_key):
        select_sql = '''
            SELECT
                spu_code_list
            FROM
                cb_guesslike_owner_rec_goods
            WHERE
                owner_area_tab_code = %s
        '''
        select_params = [stranger_key]
        with self.mysql_conn.cursor() as cur:
            count = cur.execute(select_sql, select_params)
            if not count:
                return []
            spu_li = ultimate_filter(self.area_code, cur.fetchone()[0].split(','), ignore_status=True)  # 过滤秒杀
        # 存入redis，有效期12小时
        if spu_li:
            try:
                self.pl.rpush(stranger_key, *spu_li)
                self.pl.expire(stranger_key, 43200)
                self.pl.execute()
            except:
                self._logger.exception(f'冷启动用户通用数据{stranger_key}写入redis失败')
            else:
                self._logger.info(f'冷启动用户通用数据{stranger_key}写入redis成功, 数量为{len(spu_li)}')
        return spu_li

    # 获取虚拟小区列表
    @staticmethod
    def get_virtual_areas():
        url = '{}/xwj-property-house/house/area/getVirtualAreaCodes'.format(config_dict['spring_cloud']['host_name'])
        resp_json = json.loads(requests.get(url).text)
        return resp_json.get('data', [])

    # 过滤状态不好的商品
    def filter(self, origin_spu_li):
        # 判断用于过滤的数据是否应该更新
        if not self.r_conn.exists('area_code_refresh_flag'):
            self.executor.submit(load_area_spu_to_redis, self._logger)
        self._logger.info(f'原商品个数{len(origin_spu_li)}')
        spu_set_for_filter = set(self.r_conn.lrange(self.area_code + '_A', 0, -1))
        self._logger.info(f'用户所在小区商品个数{len(spu_set_for_filter)}')
        for vir_area_code in self.get_virtual_areas():
            vir_area_spu_set = set(self.r_conn.lrange(vir_area_code + '_A', 0, -1))
            self._logger.info(f'虚拟小区{vir_area_code}下有{len(vir_area_spu_set)}个商品')
            spu_set_for_filter |= vir_area_spu_set
        self._logger.info(f'当前小区加虚拟小区总量为{len(spu_set_for_filter)}')
        filtered_spu_code_list = list(
            set(origin_spu_li).intersection(spu_set_for_filter))
        self._logger.info(f'过滤后剩余个数{len(filtered_spu_code_list)}')
        filtered_spu_code_list.sort(key=origin_spu_li.index)  # 保持原list顺序
        return filtered_spu_code_list
