import json
import random
import tornado.web
import requests
import tornado.gen
from redis import StrictRedis, ResponseError
from pymysql import connect
from tornado.concurrent import run_on_executor
from concurrent.futures.thread import ThreadPoolExecutor
from utils.load_data_task import load_area_spu_to_redis


class ForYouHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)

    def initialize(self, logger, config_result):
        self.config_dict = config_result
        self._logger = logger
        self.redis_conn = StrictRedis(config_result['redis']['ip'], config_result['redis']['port'],
                                      config_result['redis']['redis_num'], decode_responses=True)
        self.redis_expire_time = config_result['redis']['expire_time']
        self.is_last_page = False

    @tornado.gen.coroutine
    def post(self):
        result_json = yield self.handle_request()
        self._logger.info("page_json-" + result_json)
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.write(result_json)

    @run_on_executor
    def handle_request(self):
        body = json.loads(self.request.body)
        self._logger.info("input_dict-" + json.dumps(body, ensure_ascii=False))  ##用input_dict-作为分隔符
        self.page = int(body['page'])
        self.rows = int(body['rows'])
        self.ownerCode = body['ownerCode']
        self.areaCode = body['areaCode'] + '_A'
        self.top_spu_list = body['topSpuList']
        self.spuCodeList = body['spuCodeList']
        self.redis_key_for_individual = '%s_%s' % (self.ownerCode, body['areaCode'])  # 用户在redis的key
        self.redis_key_for_has_next_mysql = '%s_%s_1' % (self.ownerCode, body['areaCode'])  # mysql中是否还有下一条个性化推荐的标志
        # 获取虚拟小区列表
        # self.get_virtual_areas()
        if self.page == 1:
            spu_code_list = self.handle_refresh()
        else:
            spu_code_list = self.handle_browse_next_page()
        result_dict = {
            'resultCode': 0,
            'msg': '操作成功',
            'data': {
                'spuCodeList': spu_code_list,
                'isLastPage': self.is_last_page
            }
        }
        return json.dumps(result_dict, ensure_ascii=False)

    # 处理刷新
    def handle_refresh(self):
        if not self.redis_conn.exists(self.redis_key_for_individual):
            # 从Mysql获取数据
            filtered_spu_code_list = self.get_filtered_spu_code_from_mysql()
            # 获取的spu不足请求个数时，处理完推广商品直接返回，不存入redis
            if len(filtered_spu_code_list) <= self.rows:
                self.is_last_page = True
                return self.join_promotion_goods(filtered_spu_code_list)
        else:
            # 从redis获取数据，并加入置顶商品
            filtered_spu_code_list = self.get_filtered_individual_spu_code_from_redis()
            # 如redis中剩余spu不滿足请求个数，从Mysql获取数据拼接到后面
            if len(filtered_spu_code_list) <= self.rows:
                # 如果redis没有redis_key_for_has_next_mysql标志，拼接数据取自redis中小区下的 spu_code
                if not self.redis_conn.exists(self.redis_key_for_has_next_mysql):
                    new_spu_code_list = self.redis_conn.lrange(self.areaCode, 0, -1)
                # 有这个key，就从mysql中取拼接数据，无论这个key的值如何
                else:
                    new_spu_code_list = self.get_filtered_spu_code_from_mysql()
                # 去重
                new_spu_code_list = sorted(list(set(new_spu_code_list) - set(filtered_spu_code_list)),
                                           key=new_spu_code_list.index)
                filtered_spu_code_list.extend(new_spu_code_list)
        # 加入推广商品
        spu_code_list = self.join_promotion_goods(filtered_spu_code_list)
        pl = self.redis_conn.pipeline()
        pl.rpush(self.redis_key_for_individual, *spu_code_list[self.rows:])
        pl.expire(self.redis_key_for_individual, self.redis_expire_time)
        pl.execute()
        return spu_code_list[:self.rows]

    # 处理翻页
    def handle_browse_next_page(self):
        # redis中没有说明用户在界面停留时间过长导致数据过期，从Mysql根据请求page获取对应数据重新放入redis
        if not self.redis_conn.exists(self.redis_key_for_individual):
            filtered_spu_code_list = self.get_filtered_spu_code_from_mysql()
            if self.total_num < 400:
                # 总量小于400说明Mysql只有一条记录，根据page分页返回，超过400那这次拿到数据一定不是上一次放入redis的，所以直接从头拿数据返回
                start_index = self.rows * (self.page - 1)
                filtered_spu_code_list = filtered_spu_code_list[start_index:]
        else:
            filtered_spu_code_list = self.get_filtered_individual_spu_code_from_redis()
            # redis所存数据不满足或正好等于请求个数时，从mysql获取数据拼接后返回，把剩余数据再次存入redis（需要根据redis中的mysql标志位判断是否还有下一条数据）
            if len(filtered_spu_code_list) <= self.rows:
                # 判断mysql是否还有更多数据，如有，获取数据拼接返回，剩余存入redis
                if self.redis_conn.get(self.redis_key_for_has_next_mysql) == "1":
                    filtered_spu_code_list.extend(self.get_filtered_spu_code_from_mysql())
                else:
                    self.is_last_page = True
                    self.redis_conn.delete(self.redis_key_for_has_next_mysql)
                    return filtered_spu_code_list
        pl = self.redis_conn.pipeline()
        pl.rpush(self.redis_key_for_individual, *filtered_spu_code_list[self.rows:])
        pl.expire(self.redis_key_for_individual, self.redis_expire_time)
        try:
            pl.execute()
        except ResponseError:
            self._logger.warning('写入redis数据为空')
            self.is_last_page = True
            self.redis_conn.delete(self.redis_key_for_has_next_mysql)
        return filtered_spu_code_list[:self.rows]

    # 从redis获取过滤后的用户专属spu_code排序
    def get_filtered_individual_spu_code_from_redis(self):
        pl = self.redis_conn.pipeline()
        pl.lrange(self.redis_key_for_individual, 0, -1)
        pl.delete(self.redis_key_for_individual)
        spu_code_list, *args = pl.execute()
        return self.spu_code_filter(spu_code_list)

    # 从mysql获取spu_code
    def get_filtered_spu_code_from_mysql(self):
        mysql_conf = self.config_dict['mysql_2']
        mysql_conn = connect(
            host=mysql_conf['ip'],
            port=int(mysql_conf['port']),
            user=mysql_conf['user'],
            password=mysql_conf['password'],
            database=mysql_conf['db']
        )
        select_sql = '''
            SELECT
                redis_index,
                spu_code_list,
                has_next_record,
                total_num 
            FROM 
                cb_foryou_owner_rec_goods 
            WHERE 
                owner_area_code='{}'
                AND shall_put_redis=1
        '''.format(self.redis_key_for_individual)
        with mysql_conn.cursor() as cur:
            row_count = cur.execute(select_sql)
            # 获取不到说明用户在mysql中没有个性化推荐spu，从redis中读取key为请求小区的spu排序，无需过滤
            if not row_count:
                # 如果redis小区数据更新标志消失，说明该更新小区数据了
                # TODO:或许可以移到其他位置
                if not self.redis_conn.exists('area_code_refresh_flag'):
                    self.executor.submit(load_area_spu_to_redis, self._logger)
                self.total_num = self.redis_conn.llen(self.areaCode)
                mysql_conn.close()
                return self.redis_conn.lrange(self.areaCode, 0, -1)
            redis_index, spu_code_str, has_next_record, total_num = cur.fetchone()
        self.total_num = total_num
        spu_code_list = spu_code_str.split(',')
        # 在redis写入是否还有下一条记录的标志
        if len(spu_code_list) > self.rows:
            self.redis_conn.set(self.redis_key_for_has_next_mysql, has_next_record)
        if total_num > 400:
            self.executor.submit(self.write_to_mysql, redis_index, has_next_record, mysql_conn)
        else:
            mysql_conn.close()
        return self.spu_code_filter(spu_code_list)

    def write_to_mysql(self, current_redis_index, has_next_record, mysql_conn):
        # 本次记录放入redis标志置零
        update_sql = '''
            UPDATE
                cb_foryou_owner_rec_goods
            SET
                shall_put_redis = %s
            WHERE
                owner_area_code='{}'
                AND redis_index = %s
        '''.format(self.redis_key_for_individual)
        with mysql_conn.cursor() as cur:
            try:
                cur.execute(update_sql, [0, current_redis_index])
                # 把下一条记录的redis标志置1
                redis_index = current_redis_index + 1 if has_next_record else 1
                cur.execute(update_sql, [1, redis_index])
                mysql_conn.commit()
            except:
                self._logger.exception('redis标志位切换异常')
                # mysql_conn.rollback()
        mysql_conn.close()
                                
    # 获取虚拟小区列表
    def get_virtual_areas(self):
        # url = 'http://' + self.config_dict['spring_cloud']['ip'] + ':' + self.config_dict['spring_cloud'][
        #     'port'] + '/xwj-property-house/house/area/getVirtualAreaCodes'
        url = self.config_dict['spring_cloud']['host_name'] + '/xwj-property-house/house/area/getVirtualAreaCodes'
        resp_json = json.loads(requests.get(url).text)
        self.virtual_area_list = resp_json.get('data', [])

    # spu过滤器
    def spu_code_filter(self, origin_spu_code_list):
        # 所有虚拟小区下的spu集合
        # virtual_area_spuCode_union = set()
        # for virtual_area in self.virtual_area_list:
        #     virtual_area_spuCode_union = virtual_area_spuCode_union.union(
        #         set(self.redis_conn.lrange(virtual_area + '_A', 0, -1)))
        # filtered_spu_code_list = list(
        #     set(origin_spu_code_list).intersection(
        #         set(self.redis_conn.lrange(self.areaCode, 0, -1)).union(virtual_area_spuCode_union)))
        # 判断用于过滤的数据是否应该更新
        if not self.redis_conn.exists('area_code_refresh_flag'):
            self.executor.submit(load_area_spu_to_redis, self._logger)
        filtered_spu_code_list = list(
            set(origin_spu_code_list).intersection(set(self.redis_conn.lrange(self.areaCode, 0, -1))))
        filtered_spu_code_list.sort(key=origin_spu_code_list.index)
        return filtered_spu_code_list

    # 加入推广商品
    def join_promotion_goods(self, origin_li):
        # 推广商品过滤
        # top_goods_li = self.spu_code_filter(self.top_spu_list)
        origin_li = self.spu_code_filter(self.top_spu_list + origin_li)
        promotion_goods_li = self.spu_code_filter(self.spuCodeList)
        random.shuffle(promotion_goods_li)
        i = 0 if len(self.top_spu_list) < 3 else 1
        for e in promotion_goods_li:
            # 推广位索引
            prom_index = i * 4 + 2
            # 如当前推广商品已经出现在推广位前（算法或置顶），continue
            if e in origin_li[: prom_index]:
                continue
            try:
                origin_li.remove(e)
            except:
                pass
            origin_li.insert(prom_index, e)
            i += 1
        return origin_li
