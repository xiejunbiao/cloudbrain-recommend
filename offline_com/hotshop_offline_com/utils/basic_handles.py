import numpy as np
from collections import Counter

def sort_adict(adict):
    sorted_tuple = sorted(adict.items(),\
                        key=lambda x: x[1], reverse=True)
    sorted_dict = dict((x, y) for x, y in sorted_tuple)
    #sorted_list = list(sorted_dict.keys())
    return sorted_dict

def sort_by_count(alst,blst):
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