import time
from .tool_kit import get_redis_conn, get_mysql_conn


# 把每个小区下的商品以销量排序加载到redis，服务启动时调用一次
def load_area_spu_to_redis(logger):
    logger.info('开始加载小区数据到redis...')
    redis_conn = get_redis_conn()
    flag_key = 'area_code_refresh_flag'
    if redis_conn.exists(flag_key):
        logger.info('已有小区数据无需预加载')
        return redis_conn.close()
    redis_conn.setex(flag_key, 3600, 1)
    start_time = time.time()
    sqyn_conn = get_mysql_conn('mysql_2')
    with sqyn_conn.cursor() as cursor:
        # 获取全量商品根据销量的排序
        select_sql = '''
            SELECT
                scope.area_code,
                scope.spu_code 
            FROM
                cb_goods_spu_for_filter AS filter
                INNER JOIN cb_goods_scope AS scope
            ON
                filter.spu_code = scope.spu_code
            WHERE
                filter.goods_status = 1 
                AND filter.store_status = 1 
                AND filter.spu_name NOT REGEXP "链接|差价" 
            ORDER BY
                filter.sale_month_count DESC
        '''
        cursor.execute(select_sql)
        area_spu_dict = {}
        for area, spu in cursor.fetchall():
            area_key = area + '_A'
            spu_li = area_spu_dict.setdefault(area_key, [])
            spu_li.append(spu)
        pl = redis_conn.pipeline()
        for area_key, spu_li in area_spu_dict.items():
            logger.info(f'小区{area_key}下有{len(spu_li)}个商品')
            # 挨个地区更新，先删除再push
            pl.delete(area_key)
            pl.rpush(area_key, *spu_li)
        pl.execute()
    logger.info(f'加载小区商品到redis完成，耗时：{time.time() - start_time}')
    pl.close()
    sqyn_conn.close()
    redis_conn.close()
