# @author : zhaizhengyuan
# @datetime : 2020/8/6 16:18
# @file : st.py
# @software : PyCharm

"""
文件说明：

"""

import numpy as np
import pandas as pd
from offline_com.foryou_offline_com.utils.basic_handles import resort_by_multiple

data = {'name': ['Jack', 'Tom', 'Mary', 'Tom'], 'age': [18, 19, 21, 22], 'gender': ['m', 'm', 'w', 'M'], 'level':[2, 1, 2, 2]}
frame = pd.DataFrame(data)
data_1 = frame[frame['name'].isin(['Tom', 'Jack'])]['age'].tolist()
print(data_1)
print(frame[frame.gender=='w'].name.values[0])
ll = ['a', 'b', 'c', 'd']
lt = ['c', 'd', 'a']
lt.sort(key=ll.index)
print(lt)
zero_col_count = dict(frame['gender'].value_counts())
print(zero_col_count)
print(zero_col_count.keys())
a_dict = {"a":['a1','a2'],"b":['b1','b2','b3'],"c":['c1']}
b_list = [2, 3, 1]
a_list = resort_by_multiple(a_dict, b_list)

#print(frame[frame.name == 'Tom'].age.tolist())
tab_name1=['Mary','Tom']
l0=frame[(frame.name.isin(tab_name1)) & (frame.level == 2)].age.tolist()
c = [1,2,3,4]
d = [1,2,3,4]
c.extend(d)
div_list =[]
save_list=['a1','a2','b1','b2','b3','c1','d']
print(save_list[6:8])
for i in range(0, 7, 2):
    print(i)
    temp = save_list[i:i + 2]
    div_list.append(temp)
    # 最后一段的商品列表如果不足200，合并到上一条记录
try:
    if len(div_list[3]) < 200:
        div_list[2].extend(div_list[3])
except IndexError:
    print("The number of key is 200")
    # 根据分段情况，得到各条记录所对应的其他表字段数据
print(div_list)
for i in range(3):
    try:
        redis_index = i + 1
        spu_code_list = div_list[i]
        spu_code_list_str = ",".join(spu_code_list)
        print(spu_code_list_str)
    except Exception as e:
        print("The rec_spu_list of owner_area is NULL")