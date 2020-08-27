from utils.dataBaseHandle import readMysqlmultiPd
import numpy as np
import pandas as pd
from utils.configInit import ConfigValue
from math import ceil
from interval import Interval
import jieba.analyse
from profile.user_profile import user
from profile.goods_profile import goods
import math
from save_rec_result1 import dict_to_mysql
rootDir='D:\hisense'
config_result = {}
configv = ConfigValue(rootDir)
config_result["mysql_2"] = configv.get_config_values("mysql_2")

def divide_list(lst, n):
    if n <= 0:
        yield lst
        return
    i, div = 0, ceil(len(lst) / n)
    while i < n:
        yield lst[i * div: (i + 1) * div]
        i += 1

def jieba_keywords(spu_name):
    cixing = ('ns', 'n', 'nr', 'vn', 'v', 'i', 'a', 'e','y',\
                  'ad', 'an', 'j', 'nz', 'Ng', 'vd', 'un', 'dg','o',\
                  'ng', 'nt', 'nl', 'nr2', 's', 'f', 'vx', 'vi', 'vl', 'Ag',\
                  'vg', 'ag', 'al', 'bl', 'z', 'rz', 'r', 'b', 'l', 'g', 'nz')
    list = jieba.analyse.extract_tags(spu_name, topK=20, withWeight=False, allowPOS=cixing)
    return list

def user_consume_zoom(con_dataPd):
    all_con = con_dataPd["sum2"] / con_dataPd["sum1"]
    all_con_list = list(set(all_con.tolist()))
    all_con_list.sort()
    div_all_con_list = list(divide_list(all_con_list, 5))

    zooms = {}
    zooms["zoom_0_1"] = Interval(min(div_all_con_list[0]), max(div_all_con_list[0]))
    zooms["zoom_1_2"] = Interval(min(div_all_con_list[1]), max(div_all_con_list[1]))
    zooms["zoom_2_3"] = Interval(min(div_all_con_list[2]), max(div_all_con_list[2]))
    zooms["zoom_3_4"] = Interval(min(div_all_con_list[3]), max(div_all_con_list[3]))
    zooms["zoom_4_5"] = Interval(min(div_all_con_list[4]), max(div_all_con_list[4]))
    return zooms

def zoom_gen(all_data_list):
    #all_data_list = list(set(data.tolist()))
    all_data_list.sort()
    div_all_data_list = list(divide_list(all_data_list, 5))

    zooms = {}
    zooms["zoom_0_1"] = Interval(min(div_all_data_list[0]), max(div_all_data_list[0]))
    zooms["zoom_1_2"] = Interval(min(div_all_data_list[1]), max(div_all_data_list[1]))
    zooms["zoom_2_3"] = Interval(min(div_all_data_list[2]), max(div_all_data_list[2]))
    zooms["zoom_3_4"] = Interval(min(div_all_data_list[3]), max(div_all_data_list[3]))
    zooms["zoom_4_5"] = Interval(min(div_all_data_list[4]), max(div_all_data_list[4]))
    return zooms

def all_spu_keywords(dataPd):

    all_spu_list = dataPd["spu_code"].tolist()
    #all_spu_list = list(set(all_spu))
    spu_keywords_dict = {}
    for spu_code in all_spu_list:
        spu_name = dataPd[dataPd["spu_code"] == spu_code]["spu_name"].values[0]
        spu_keywords = jieba_keywords(spu_name)
        spu_keywords_dict[spu_code] = spu_keywords
    return spu_keywords_dict

def keywords_match(all_keywords_dict,user_data,item_data):
    user_spu_code_list = user_data["spu_code"].tolist()
    item_spu_code = item_data["spu_code"].values[0]
    item_spu_keywords = all_keywords_dict[item_spu_code]
    user_item_score = 0
    for user_spu_code in user_spu_code_list:
        user_spu_keywords = all_keywords_dict[user_spu_code]
        if user_spu_code == item_spu_code:
            user_item_score += user_data[user_data["spu_code"]==user_spu_code]["spu_num"].values[0]
        else:
            num_of_same_keywords = len([x for x in user_spu_keywords if x in item_spu_keywords])
            num_of_all_keywords = len(set(user_spu_keywords+item_spu_keywords))

            user_item_score += num_of_same_keywords/num_of_all_keywords * user_data[user_data["spu_code"]==user_spu_code]["spu_num"].values[0]
    return user_item_score

def sort_adict(adict):
    sorted_tuple = sorted(adict.items(),\
                        key=lambda x: x[1], reverse=True)
    sorted_dict = dict((x, y) for x, y in sorted_tuple)
    sorted_list = list(sorted_dict.keys())
    return sorted_list

def user_spu(user_spu_code_list,user_spu_data):
    user_code_data = pd.DataFrame(columns=("spu_code","spu_name","spu_num"))
    for spu_code in user_spu_code_list:
        spu_num = len(user_spu_data[user_spu_data["spu_code"]==spu_code])
        spu_name = user_spu_data[user_spu_data["spu_code"]==spu_code]["spu_name"].tolist()[0]
        user_code_data = user_code_data.append(pd.DataFrame({"spu_code":[spu_code],"spu_name":[spu_name],"spu_num":[spu_num]}))
    return user_code_data


mysqlCmd = "SELECT * FROM cb_owner_buy_goods_info"
dataPd = readMysqlmultiPd(mysqlCmd,config_result["mysql_2"])
mysqlCmd = "SELECT * FROM cb_goods_spu_for_filter where goods_status=1"
dataPd1 = readMysqlmultiPd(mysqlCmd,config_result["mysql_2"])
mysqlCmd = "SELECT * FROM cb_goods_scope"
area_dataPd = readMysqlmultiPd(mysqlCmd,config_result["mysql_2"])


dataPd_all = dataPd[["spu_code","spu_name"]].append(dataPd1[["spu_code","spu_name"]])
dataPd_all = dataPd_all.drop_duplicates()
dataPd_all.index=list(range(dataPd_all.shape[0]))
all_keywords_dict = all_spu_keywords(dataPd_all)
mysqlCmd = "SELECT owner_code, sum(sku_count) as sum1,sum(real_sku_total) as sum2 FROM cb_owner_buy_goods_info group by owner_code"
con_dataPd = readMysqlmultiPd(mysqlCmd,config_result["mysql_2"])

owner_list = dataPd["owner_code"].tolist()
owner_list = list(set(owner_list))

all_cate = dataPd["first_cate"].tolist()
all_cate_list = list(set(all_cate))
all_cate_list.sort()

all_shop = dataPd["shop_code"].tolist()
all_shop_list = list(set(all_shop))
all_shop_list.sort()
shop_data = dataPd[['spu_code','shop_code']]

all_spu_list = dataPd1["spu_code"].tolist()
owner_area_rec = {}

for owner_code in owner_list:
    print(owner_code)

    owner_fea = user(owner_code,dataPd,con_dataPd,all_cate_list,all_shop_list)
    #owner_fea.user_consume()
    area_list = dataPd[dataPd["owner_code"] == owner_code]["rec_area_code"].tolist()
    area_list = list(set(area_list))
    for area_code in area_list:
        area_spu_list = area_dataPd[area_dataPd["area_code"] == area_code]["spu_code"].tolist()
        fil_area_spu_list = list(set(area_spu_list) & set(all_spu_list))
        area_spu_dict = {}
        for spu_code in fil_area_spu_list:
            spu_fea = goods(spu_code,shop_data,dataPd1,all_cate_list,all_shop_list)
            owner_spu_score1 = 5-abs(owner_fea.user_con_score-spu_fea.item_price_score)
            owner_spu_score2 = np.dot(owner_fea.user_cate_score,spu_fea.item_cate_score)
            owner_spu_score3 = np.dot(owner_fea.user_shop_score, spu_fea.item_shop_score)
            owner_spu_score4 = keywords_match(all_keywords_dict,owner_fea.user_code_data,spu_fea.item_code_data)
            owner_spu_score = 1/math.exp(owner_fea.user_con_score)*owner_spu_score1+owner_spu_score2+0.1*owner_spu_score3+owner_spu_score4
            area_spu_dict[spu_code] = owner_spu_score

        owner_area_rec[owner_code+"_"+area_code] = sort_adict(area_spu_dict)

dict_to_mysql(owner_area_rec,config_result["mysql_2"])
# all_price = dataPd1['sale_price']
# price_zoom = zoom_gen(all_price)
# print(price_zoom)
# all_category = dataPd['first_cate'].tolist()
# all_category_list = list(set(all_category))
# all_category_list.sort()

#all_spu_name_list = [dataPd[dataPd['spu_code']==code][['spu_name']] for code in all_spu_list]
#print(len(all_spu_name_list))
#category_num_list = [len(dataPd[dataPd['first_cate']==cate]) for cate in all_category_list]
# user_category_num_list = [len(dataPd[(dataPd['owner_code']=='*2019041602112879202')&(dataPd['first_cate']==cate)]) for cate in all_category_list]
#print([num/sum(user_category_num_list) for num in user_category_num_list])
# user_category_data = dataPd[dataPd['owner_code'] == owner][['spu_code', 'first_cate']]
# user_shop_data = dataPd[dataPd['owner_code'] == owner][['spu_code', 'shop_code']]
# user_spu_data = dataPd[dataPd['owner_code'] == owner][['spu_code', 'spu_name']]
# spu_code_list = dataPd[dataPd['owner_code'] == owner]['spu_code'].tolist()

# user_consume_data = dataPd[dataPd['owner_code'] == '*2019041602112879202'][['sku_count','real_sku_total']]
# user_category_data = dataPd[dataPd['owner_code'] == '*2019041602112879202'][['spu_code','first_cate']]
# user_shop_data = dataPd[dataPd['owner_code'] == '*2019041602112879202'][['spu_code','shop_code']]
# user_spu_data = dataPd[dataPd['owner_code'] == '*2019041602112879202'][['spu_code','spu_name']]
# user_spu_code_list = dataPd[dataPd['owner_code'] == '*2019041602112879202']['spu_code'].tolist()
# user_spu_code_list = list(set(user_spu_code_list))
# user_spu_dict = user_spu(user_spu_code_list,user_spu_data)
#
# print(user_spu_dict)
# print(user_spu_dict.sort_values(axis=0,ascending=False,by=['spu_num']))
#

#print(user_consume_data.sum(axis=0)['sku_co
#dataPd['spu_code'].tolist()unt'])