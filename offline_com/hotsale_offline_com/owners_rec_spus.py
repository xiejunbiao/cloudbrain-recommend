# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 17:52:16 2020

@author: zhaizhengyuan
"""
from hotsale_offline_com.utils.basic_handles import buy_times,fileter_list
from hotsale_offline_com.utils.dataBaseHandle import readMysqlmultiPd
from hotsale_offline_com.get_owner_sec_dict import get_owner_sec_spu_list
from hotsale_offline_com.get_owner_sec_dict import sort_all_second_goods
from hotsale_offline_com.all_spus_keywords import get_all_goods_keywords_dict

##计算用户的购买商品字典以及购买商品列表
def get_owner_items_totalcount(dataDf,owner):

    ##计算该用户购买过的商品名称列表
    owner_dataDf = dataDf['owner_code'].tolist()
    owner_list_index = [i for i,x in enumerate(owner_dataDf) if x==owner]
    ##用户购买的商品字典，键是商品名称，值是购买次数
    owner_total_dict = buy_times(dataDf,'spu_code',owner_list_index)
    ##用户购买过的商品列表
    owner_items_list = list(owner_total_dict.keys())
    return owner_total_dict,owner_items_list

##用户购买和没有购买的一级类别列表
##输入是用户购买的商品列表
def buy_first_cate(dataDf,spu_code_list):

    spucode_dataDf = dataDf['spu_code'].tolist()
    first_dataDf = dataDf['first_cate'].tolist()
    first_spu_dict = {}
    for spu_code in spu_code_list:
        first_cate = first_dataDf[spucode_dataDf.index(spu_code)]
        first_spu_dict.setdefault(first_cate, []).append(spu_code)
    ##用户购买的一级类别列表
    #buy_first_list = list(first_spu_dict.keys())
    ##用户没有购买的一级类别列表
    # notbuy_first_list = list(set(first_dataDf).difference(set(buy_first_list)))
    return first_spu_dict

def get_not_first_rec_dict(first_cate,dataDf):

    all_first_cate_data = dataDf['first_cate'].tolist()
    all_spu_code_data = dataDf['spu_code'].tolist()
    first_cate_index = [index for (index,value) in enumerate(all_first_cate_data)\
                        if value == first_cate]
    first_spu_data = [all_spu_code_data[i] for i in first_cate_index]
    return first_spu_data

def get_not_order_rec_dict(dataDf):

    owner_rec_code_dict = {}
    first_cate_list = list(set(dataDf['first_cate'].tolist()))
    for first_cate in first_cate_list:
        spu_code_list = get_not_first_rec_dict(first_cate,dataDf)
        owner_rec_code_dict[first_cate] = spu_code_list
    return owner_rec_code_dict

##计算一个购买3次以上的用户推荐结果字典
def get_rec_owner_dict(owner,owner_rec_code_dict,dataDf1,\
                       goods_keywords_dict,cate_goods_dict):
    #计算用户的购买商品字典和购买商品列表
    owner_item_result = get_owner_items_totalcount(dataDf1,owner)
    owner_total_dict = owner_item_result[0]
    owner_allitems_list = owner_item_result[1]

    #根据用户的购买商品列表，得到用户购买的一级类别和没有购买的一级类别列表
    buy_result = buy_first_cate(dataDf1,owner_allitems_list)
    owner_items_dict = buy_result
    #print(owner_items_dict['100001'])
    # owner_not_buy_first_list = buy_result[1]

    # ##对用户没有购买过的一级类别列表，直接根据商品销量计算推荐结果
    # for first_cate in owner_not_buy_first_list:
    #     #按照该类别下的月销量对商品进行排序，得到推荐商品列表
    #     owner_sec_code_list = get_not_first_rec_dict(first_cate,dataDf)
    #     owner_rec_code_dict[first_cate] = owner_sec_code_list

    ##对用户购买的一级类别列表，根据算法计算推荐结果
    for first_cate in list(owner_items_dict.keys()):
        #用户购买的该一级类别下的商品列表
        owner_first_items_list = owner_items_dict[first_cate]
        #print(owner_first_items_list)
        #计算给用户推荐的二级类别下的商品列表
        owner_sec_code_list = get_owner_sec_spu_list(first_cate,cate_goods_dict,\
                        owner_first_items_list,goods_keywords_dict,owner_total_dict)
        #mysql = "SELECT distinct spu_code FROM cb_goods_scope where area_code not in " + \
        #        "(select rec_area_code from cb_owner_buy_goods_info where owner_code='%s')" % owner
        #owner_area_data = readMysqlmultiPd(mysql, config)
        #owner_spus_list = owner_area_data['spu_code'].tolist()
        #owner_area_sec_code_list = filter(lambda n: n not in owner_spus_list, owner_sec_code_list)
        owner_rec_code_dict[first_cate] = owner_sec_code_list#fileter_list(owner_sec_code_list,owner_spus_list)
    return owner_rec_code_dict

##计算所有用户的推荐结果字典
def get_owners_rec_spus_dict(dataDf,dataDf1):

    #先得到订单表中每个用户的购买次数字典
    rec_owners_dict = buy_times(dataDf1,'owner_code','')
    #得到所有商品的关键词列表、商品的类别字典
    goods_result = get_all_goods_keywords_dict(dataDf,dataDf1)
    #初始化空的所有用户的推荐商品字典
    owners_rec_codes_dict = {}
    for owner in list(rec_owners_dict.keys()):
        #所有商品的关键词列表字典
        goods_keywords_dict = goods_result[0]
        #print(goods_keywords_dict)
        #所有商品的类别字典
        cate_goods_dict = goods_result[1]
        #print(cate_goods_dict)
        #初始化空的用户的推荐商品字典
        owner_rec_code_dict = {}
        #只为购买次数大于3的用户计算推荐结果
        if rec_owners_dict[owner] > 3:
            #计算该用户的推荐结果字典
            #键是一级类别，值是spu_code list
            owner_rec_code_dict = get_rec_owner_dict(owner, owner_rec_code_dict,dataDf1, \
                                                     goods_keywords_dict, cate_goods_dict)
            owners_rec_codes_dict[owner] = owner_rec_code_dict
        else:
            continue#owners_rec_codes_dict[owner] = get_not_order_rec_dict(dataDf)
    #aa=owners_rec_codes_dict
    return owners_rec_codes_dict