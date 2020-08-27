#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time      :2020/8/5 11:26
# @Author    :XieYuHao

import json
import math

import requests
from redis import StrictRedis
from .configInit import config_dict
from pymysql import connect


# 分页器
def paginator(page: int, rows: int, origin_list: list) -> dict:
    """
    :param page: 请求页数
    :param rows: 请求个数
    :param origin_list: 需要被分页的list
    :return: 分页后的list
    """
    start_index = (page - 1) * rows
    total = len(origin_list)
    data = {
        "current": page,  # 当前页
        "pages": math.ceil(total / rows),  # 总页数
        "records": origin_list[start_index: start_index + rows],
        'searchCount': True,
        "size": rows,  # 分页行数
        "total": total  # 数据总量
    }
    return data


# 过滤器(会过滤掉 秒杀、下架、0库存)
def ultimate_filter(area_code: str, spu_li: list, ignore_status: bool = False) -> list:
    """
    :param area_code: 小区code
    :param spu_li: 需要被过滤的list
    :param ignore_status: 是否忽略过滤库存和上下架, 默认false
    :return: 过滤后的list
    """
    if not ignore_status:
        r_conn = get_redis_conn()
        # 过滤下架和库存不足的商品
        spu_set_for_filter = set(r_conn.lrange(area_code + '_A', 0, -1))
        for vir_area_code in get_virtual_areas():
            spu_set_for_filter |= set(r_conn.lrange(vir_area_code + '_A', 0, -1))
        filtered_spu_code_list = list(set(spu_li).intersection(spu_set_for_filter))
        spu_li = sorted(filtered_spu_code_list, key=spu_li.index)  # 保持原list顺序
        r_conn.close()
    # 请求其他服务的接口过滤掉秒杀商品
    url = config_dict['spring_cloud']['host_name'] + '/xwj-commerce-seckill/activitySpu/getNoSeckillSpuCodes'
    data = {
        "areaCode": area_code,
        "spuCodes": spu_li
    }
    res = requests.post(url, json=data)
    filtered_spu_code_list = json.loads(res.text).get('data', [])
    return filtered_spu_code_list if filtered_spu_code_list else []


# 获取mysql连接
def get_mysql_conn(mysql_tag: str):
    """
    :param mysql_tag: mysql_1(hisense)或mysql_2(sqyn)
    :return: mysql connect
    """
    mysql_conf = config_dict[mysql_tag]
    return connect(
        host=mysql_conf['ip'],
        port=int(mysql_conf['port']),
        user=mysql_conf['user'],
        password=mysql_conf['password'],
        database=mysql_conf['db'],
    )


# 获取redis连接
def get_redis_conn():
    redis_conf = config_dict['redis']
    return StrictRedis(redis_conf['ip'], redis_conf['port'], int(redis_conf['redis_num']), decode_responses=True)


# 获取虚拟小区列表
def get_virtual_areas():
    url = '{}/xwj-property-house/house/area/getVirtualAreaCodes'.format(config_dict['spring_cloud']['host_name'])
    resp_json = json.loads(requests.get(url).text)
    return resp_json.get('data', [])


# 保序去重
def remove_duplication(origin_spu_li):
    return sorted(list(set(origin_spu_li)), key=origin_spu_li.index)
