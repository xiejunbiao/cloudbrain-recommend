import sys

sys.path.append("../") ## 会从上一层目录找到一个package
from hotsale_offline_com.utils.basic_handles import sort_adict

def get_item_cate_salecount(item,goods_keywords_dict,owner_total_dict):

    owner_item_dict = goods_keywords_dict[item]
    item_cate_code = list(owner_item_dict.keys())[1]
    item_name_keywords = list(owner_item_dict.keys())[2]
    ##获取该商品的销量
    #item_sale_count = owner_item_dict[item_spu_code]
    ##获取该一级类别的销量
    cate_item_salecount = owner_item_dict[item_cate_code]
    owner_item_keywords_list = owner_item_dict[item_name_keywords]
    ##用户购买该商品的次数
    owner_item_buycount = owner_total_dict[item]
    return owner_item_keywords_list,cate_item_salecount,\
           owner_item_buycount

def get_match_salecount_keywords_result(match_item,goods_keywords_dict):
    match_item_dict = goods_keywords_dict[match_item]
    match_spu_name = list(match_item_dict.keys())[0]
    match_name_keywords = list(match_item_dict.keys())[2]
    match_item_salecount = match_item_dict[match_spu_name]
    match_item_keywords_list = match_item_dict[match_name_keywords]
    return match_item_keywords_list,match_item_salecount

##计算两个商品名称的匹配分数
def key_words_match_score(name_list1,name_list2):

    #计算两个列表的交集
    ret_list = list(set(name_list1) & set(name_list2))
    #计算两个列表的并集，并去掉重复元素
    hebing_list = name_list1 + name_list2
    hebing_list = list(set(hebing_list))
    #计算两个列表的匹配分数
    score=len(ret_list) / len(hebing_list)
    return score

def sort_all_second_goods(sec_goods_dict,sec_code_score_dict):

    all_sorted_list = []
    if sec_code_score_dict =={}:
        sec_cates_list = list(sec_goods_dict.keys())
    else:
        sorted_sec_code_dict1 = sort_adict(sec_code_score_dict)
        sec_cates_list = list(sorted_sec_code_dict1.keys())
    val_len = []
    for value in sec_goods_dict.values():
        val_len.append(len(value))
    max_len = max(val_len)
    for i in range(max_len):
       for sec_cate in sec_cates_list:
           try:
              all_sorted_list.append(sec_goods_dict[sec_cate][i])
           except IndexError:
              continue
    #print(all_sorted_list)
    return all_sorted_list

##计算给用户推荐的二级类别商品列表
##cate_goods_dict商品类别字典
##owner_first_items_list用户购买的该一级类别下的商品列表
##goods_keywords_dict商品关键词字典
##owner_total_dict用户的商品购买次数字典
def get_owner_sec_spu_list(first_cate,cate_goods_dict,owner_first_items_list,\
                       goods_keywords_dict,owner_total_dict):

    owner_total_buycount = sum(owner_total_dict.values())
    owner_sec_code_dict = {}
    sec_code_score_dict = {}

    sec_cate_spus_dict = cate_goods_dict[first_cate]
    second_cate_codes_list = list(sec_cate_spus_dict.keys())
    for second_cate in second_cate_codes_list:
        sec_spu_code_list = sec_cate_spus_dict[second_cate]
        owner_spu_code_dict = {}
        for match_item in sec_spu_code_list:

            match_result = get_match_salecount_keywords_result(match_item, \
                                                         goods_keywords_dict)
            for spu_code in owner_first_items_list:
                spu_result = get_item_cate_salecount(spu_code, goods_keywords_dict, \
                                                     owner_total_dict)
                try:
                    match_score = key_words_match_score(match_result[0], spu_result[0])
                except ZeroDivisionError:
                    match_score = 1
                rec_score = 0.001 * match_result[1] / spu_result[1] + \
                            (spu_result[2] / owner_total_buycount) * match_score*100
                owner_spu_code_dict.setdefault(match_item, []).append(rec_score)
        for key, value in owner_spu_code_dict.items():
            try:
                owner_spu_code_dict[key] = sum(value)
            except TypeError:
                owner_spu_code_dict[key] = value
        sorted_spu_code_dict = sort_adict(owner_spu_code_dict)
        #print(sorted_spu_code_dict)
        #print(list(sorted_spu_code_dict.keys()))
        #print(list(sorted_spu_code_dict.values())[0])
        owner_sec_code_dict[second_cate] = list(sorted_spu_code_dict.keys())
        sec_code_score_dict[second_cate] = list(sorted_spu_code_dict.values())[0]
    owner_sec_code_list = sort_all_second_goods(owner_sec_code_dict, sec_code_score_dict)
    return owner_sec_code_list