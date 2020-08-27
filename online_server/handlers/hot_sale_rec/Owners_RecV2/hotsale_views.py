import json
import math
import threading
from pymysql import connect
from redis import StrictRedis


class HotSale(object):
    def __init__(self, spuCodeDict, config_result):
        self.sortMethod = spuCodeDict['sortMethod']
        self.ownerCode = spuCodeDict['ownerCode']
        self.areaCode = spuCodeDict['areaCode']
        self.cateCode = spuCodeDict['cateCode']
        self.secondCateCode = spuCodeDict['secondCateCode']
        self.fixedSpuCodeList = spuCodeDict['spuCodeList']
        self.salePercent = spuCodeDict['salePercent']
        self.page = int(spuCodeDict['page'])
        self.rows = int(spuCodeDict['rows'])
        self.start_row = (self.page - 1) * self.rows
        self.m_ip = config_result['mysql_2']['ip']
        self.m_port = config_result['mysql_2']['port']
        self.m_db = config_result['mysql_2']['db']
        self.m_user = config_result['mysql_2']['user']
        self.m_pw = config_result['mysql_2']['password']
        self.r_ip = config_result['redis']['ip']
        self.r_port = config_result['redis']['port']
        self.expire_time = config_result['redis']['expire_time']

    # 处理请求
    def handle_request(self):
        # redis取值优先
        redis_conn = StrictRedis(host=self.r_ip, port=self.r_port, decode_responses=True)
        if self.sortMethod == '0':
            key = '%s_%s_%s' % (self.ownerCode, self.areaCode, self.secondCateCode)
        else:
            key = '%s_%s_%s' % (self.areaCode, self.cateCode, self.sortMethod)
        spu_code_list = redis_conn.lrange(key, 0, -1)
        # redis不存在则从mysql获取
        if not spu_code_list:
            spu_code_list = self.get_result_from_mysql(redis_conn)
        spu_code_list = self.spu_code_filter(spu_code_list, redis_conn)
        pageSize = math.ceil(len(spu_code_list) / self.rows)  # 总页数
        query_list = spu_code_list[self.start_row: self.start_row + self.rows]
        return self.result_dict(pageSize, query_list)

    # 查询数据库返回数据并存入redis
    def get_result_from_mysql(self, redis_conn):
        if self.sortMethod == '0':
            all_spu_code_list = self.recommended_sort()
        else:
            all_spu_code_list = self.ordinary_sort()
        spu_code_list = all_spu_code_list[0: math.ceil(len(all_spu_code_list) * int(self.salePercent) / 100)]
        if not spu_code_list:
            return spu_code_list
        # 使用异步将mysql查询结果存入redis
        t1 = threading.Thread(target=self.save_to_redis, args=(redis_conn, spu_code_list))
        t1.start()
        return spu_code_list

    # 存入redis
    def save_to_redis(self, redis_conn, spu_code_list):
        spu_code_list = spu_code_list[0: 5000]
        if self.sortMethod == '0':
            key = '%s_%s_%s' % (self.ownerCode, self.areaCode, self.secondCateCode)
        else:
            key = '%s_%s_%s' % (self.areaCode, self.cateCode, self.sortMethod)
        # 使用管道
        pl = redis_conn.pipeline()
        pl.rpush(key, *spu_code_list)
        pl.expire(key, self.expire_time)
        pl.execute()

    # 推荐排序
    def recommended_sort(self):
        sql_for_recommended_sort = '''SELECT spu_code_list FROM cb_hotsale_owner_rec_goods 
            WHERE owner_code='%s' 
            AND first_cate=%s''' % (self.ownerCode, self.cateCode)
        recommend_sort_list = self.query_mysql(sql_for_recommended_sort)
        # 推荐表查不到的以销量降序排序
        if not recommend_sort_list:
            sql_for_saleCount_sort = '''SELECT group_concat(spu_code ORDER BY sale_month_count desc) FROM cb_goods_spu_for_filter
                WHERE goods_status=1
                    AND store_status=1
                    AND spu_code IN (SELECT spu_code FROM cb_goods_scope WHERE area_code='%s')
                GROUP BY first_cate HAVING first_cate=%s''' % (self.areaCode, self.cateCode)
            saleCount_sort_list = self.query_mysql(sql_for_saleCount_sort)
            if not saleCount_sort_list:
                return saleCount_sort_list
            # 加入置顶商品后去重、排序
            if self.fixedSpuCodeList:
                verbose_saleCount_sort_list = self.fixedSpuCodeList + saleCount_sort_list
                #saleCount_sort_list = list(set(verbose_saleCount_sort_list))
                saleCount_sort_list.sort(key=verbose_saleCount_sort_list.index)
            return saleCount_sort_list
        # 过滤掉热卖推荐中状态不满足的spu_code
        sql_for_filter = '''SELECT group_concat(spu_code) FROM cb_goods_spu_for_filter
            WHERE goods_status=1
                AND store_status=1
                AND spu_code IN (SELECT spu_code FROM cb_goods_scope WHERE area_code='%s')
            GROUP BY first_cate HAVING first_cate=%s''' % (self.areaCode, self.cateCode)
        filtered_spuCode_list = self.query_mysql(sql_for_filter)
        if not filtered_spuCode_list:
            return filtered_spuCode_list
        # 加入置顶商品
        if self.fixedSpuCodeList:
            recommend_sort_list = self.fixedSpuCodeList + recommend_sort_list
        filtered_recommend_intersection = list(set(recommend_sort_list) & set(filtered_spuCode_list))
        filtered_recommend_intersection.sort(key=recommend_sort_list.index)
        # filtered_spuCode_list.sort(key=recommend_sort_list.index)
        filtered_recommend_intersection.extend(set(filtered_spuCode_list) - set(recommend_sort_list))
        return filtered_recommend_intersection

    # 普通排序
    def ordinary_sort(self):
        if self.sortMethod == '1':
            order_by = 'sale_price'
        elif self.sortMethod == '2':
            order_by = 'sale_price desc'
        else:
            order_by = 'sale_month_count desc'
        sql = '''SELECT group_concat(spu_code order by %s) FROM cb_goods_spu_for_filter
            WHERE goods_status=1
                AND store_status=1
                AND spu_code IN (SELECT spu_code FROM cb_goods_scope WHERE area_code='%s')
            GROUP BY first_cate HAVING first_cate=%s''' % (order_by, self.areaCode, self.cateCode)
        return self.query_mysql(sql)

    # 请求mysql
    def query_mysql(self, sql):
        conn = connect(host=self.m_ip,
                       port=int(self.m_port),
                       database=self.m_db,
                       user=self.m_user,
                       password=self.m_pw)
        with conn.cursor() as cursor:
            cursor.execute(sql)
            row_one = cursor.fetchone()
        if row_one:
            row_one = row_one[0].split(',')
            return row_one
        return []

    # spu过滤器
    def spu_code_filter(self, origin_spu_code_list, redis_conn):
        # 所有虚拟小区下的spu集合
        # virtual_area_spuCode_union = set()
        # for virtual_area in self.virtual_area_list:
        #     virtual_area_spuCode_union = virtual_area_spuCode_union.union(
        #         set(self.redis_conn.lrange(virtual_area + '_A', 0, -1)))
        # filtered_spu_code_list = list(
        #     set(origin_spu_code_list).intersection(
        #         set(self.redis_conn.lrange(self.areaCode, 0, -1)).union(virtual_area_spuCode_union)))
        filtered_spu_code_list = list(
            set(origin_spu_code_list).intersection(set(redis_conn.lrange(self.areaCode + '_A', 0, -1))))
        filtered_spu_code_list.sort(key=origin_spu_code_list.index)
        return filtered_spu_code_list


    # 响应结果处理
    def result_dict(self, pageSize, query_list):
        data_dict = {
            "searchRult": "1",
            "pageNum": self.page,  # 第几页
            "pageSize": pageSize,  # 总页数
            "size": 1,
            "startRow": self.start_row + 1,  # 从1开始
            "endRow": self.start_row + self.rows,
            "total": 1,
            "pages": 1,
            "spuCodeList": query_list,  # 改成list
            "prePage": 0,
            "nextPage": 0,
            "isFirstPage": self.page == 1,
            "isLastPage": self.page >= pageSize,  # 请求页数是否等于page_size
            "hasPreviousPage": self.page != 1,
            "hasNextPage": self.page < pageSize,  # 请求的页数是否等于最后一页，不是为true
            "navigatePages": 8,
            "navigatepageNums": [1],
            "navigateFirstPage": 1,
            "navigateLastPage": 1,
            "firstPage": 1,
            "lastPage": 1
        }
        page_dict = {
            "resultCode": "0",
            "msg": "操作成功",
            "data": data_dict
        }
        return json.dumps(page_dict,ensure_ascii=False)
