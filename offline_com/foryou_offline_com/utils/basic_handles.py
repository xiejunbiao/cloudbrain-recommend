import json
import requests
import numpy as np
import jieba.analyse
from math import ceil
from interval import Interval
from collections import Counter


def sort_adict(adict):
    """
    方法用于字典元素排序
    输入：字典
    输出：排序后的字典
    功能：将字典键值按值从大到小排序
    """
    sorted_tuple = sorted(adict.items(), key=lambda x: x[1], reverse=True)
    sorted_dict = dict((x, y) for x, y in sorted_tuple)
    return sorted_dict


def keywords_extra(spu_name):
    """
    方法用于提取商品名称中的关键词
    输入：商品名称
    输出：商品关键词列表
    功能：按照词性提取商品名称关键词
    """
    ci_xing = (
        'ns', 'nr', 'vn', 'nz', 'ng', 'nr2', 'n', 'a', 'e', 'b',
        'ad', 'an', 'Ng', 'vd', 'un', 'rz', 'f', 'l', 'v', 'o',
        'ng', 'nt', 'nl', 'ag', 'dg', 'vx', 'j', 'i', 'g', 'r',
        'Ag', 'vg', 'vl', 'al', 'bl', 'vi', 's', 'y', 'z'
       )
    keywords_list = jieba.analyse.extract_tags(
        spu_name, topK=20, withWeight=False, allowPOS=ci_xing)
    return keywords_list


def zoom_gen(all_data_list):
    """
    方法用于将一个list的数据划分成5个区间
    输入：列表数据
    输出：区间字典
    功能：对列表数据划分成5个区间
    """
    if len(list(set(all_data_list))) >= 5:
        all_data_list = list(set(all_data_list))
    all_data_list.sort()
    div_all_data_list = list(divide_list(all_data_list, 5))

    zooms = {}
    try:
        zooms["zoom_0_1"] = Interval(min(div_all_data_list[0]), max(div_all_data_list[0]))
    except ValueError:
        return zooms
    try:
        zooms["zoom_1_2"] = Interval(min(div_all_data_list[1]), max(div_all_data_list[1]))
    except ValueError:
        return zooms
    try:
        zooms["zoom_2_3"] = Interval(min(div_all_data_list[2]), max(div_all_data_list[2]))
    except ValueError:
        return zooms
    try:
        zooms["zoom_3_4"] = Interval(min(div_all_data_list[3]), max(div_all_data_list[3]))
    except ValueError:
        return zooms
    try:
        zooms["zoom_4_5"] = Interval(min(div_all_data_list[4]), max(div_all_data_list[4]))
    except ValueError:
        return zooms
    return zooms


def divide_list(a_list, n):
    """
    方法用于将一个列表n等分
    输入：列表
    输出：等分list
    功能：列表n等分
    """
    if n <= 0:
        yield a_list
        return
    i, div = 0, ceil(len(a_list) / n)
    while i < n:
        yield a_list[i * div: (i + 1) * div]
        i += 1


def grade_data(zooms, data):
    """
    方法用于对data根据打分区间zooms进行打分
    输入：打分区间zooms, 打分数据data
    输出：得分score
    功能：数据打分
    """
    if data in zooms["zoom_0_1"]:
        score = 1
    elif data in zooms["zoom_1_2"]:
        score = 2
    elif data in zooms["zoom_2_3"]:
        score = 3
    elif data in zooms["zoom_3_4"]:
        score = 4
    else:
        score = 5
    return score


def get_match_score(list_1, list_2):
    """
    方法用于计算两个list的匹配程度
    输入：两个list
    输出：float值，两个list匹配分数
    功能：计算列表匹配分数
    """
    # 计算两个列表的交集
    ret_list = list(set(list_1) & set(list_2))
    # 计算两个列表的并集，并去掉重复元素
    he_bing_list = list(set(list_1 + list_2))
    # 计算两个列表的匹配分数
    try:
        match_score = len(ret_list) / len(he_bing_list)
    except ZeroDivisionError:
        match_score = 1
    return match_score


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
                all_sorted_list.append(a_dict.get(cate)[i])
            except IndexError:
                continue
    return all_sorted_list


def get_virtual_area(config):
    """
    方法用于获取不同环境下的虚拟小区代码
    输入：不同环境的配置字典
    输出：虚拟小区代码
    功能：获得虚拟小区代码
    """
    url = "{}/xwj-property-house/house/area/getVirtualAreaCodes".format(config['host_name'])
    resp_json = json.loads(requests.get(url).text)
    return resp_json.get('data', [])


def sort_by_count(alst, blst):
    blst_count =[]
    for i in blst:
        blst_count.append(alst.count(i))
    zipped = zip(blst, blst_count)
    sort_zipped = sorted(zipped, key=lambda x:x[1],reverse=True)
    result = zip(*sort_zipped)
    x_axis, y_axis = [list(x) for x in result]
    return x_axis

##将mysql读取的dataframe数据转化成对应的列表数据
def dataDf_to_list(dataDf,ziduan_name):

    #mysql列表名所对应的dataframe数据
    new_dataDf = dataDf[ziduan_name]
    #通过numpy和tolist将dataframe数据转化成list
    new_dataDf = np.array(new_dataDf)
    new_dataDf = new_dataDf.tolist()
    return new_dataDf

##用Counter统计购买次数，name可为spu_name,spu_code,cate_code,owner_code等等
##返回的是name属性下所有项的购买次数字典
##list_index为用户(或商品、或类别)数据列表的索引，用于统计某个用户的购买商品及次数
def buy_times(dataDf,ziduan_name,list_index):

    params_number = buy_times.__code__.co_argcount
    dataDf0 = dataDf[ziduan_name].tolist()
    if list_index == '':
        new_dataDf = dataDf0
    else:
        new_dataDf = [dataDf0[i] for i in list_index]
    name_total = Counter(new_dataDf)
    total_dict = dict(name_total)
    return total_dict

def list_to_str(first_cate,code_dict):
    code_list = code_dict[first_cate]
    code_list_set = list(set(code_list))
    code_list_set.sort(key=code_list.index)
    code_list_str = ",".join(code_list_set)
    return code_list_str

##选择字典中键值大于3的主键，用于过滤购买次数的小于3的用户
def selectedDictitems(adict):
    items = adict.items()
    for key in list(adict.keys()):
        if adict.get(key)<3:
            del adict[key]
    return adict

def fileter_list(list1,list2):

    jiao_set = list(set(list1) & set(list2))
    jiao_list = sorted(jiao_set, key=list1.index)
    return jiao_list