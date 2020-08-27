# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 17:52:16 2020

@author: zhaizhengyuan
"""
def list_to_str(first_cate,code_dict):
    code_list = code_dict[first_cate]
    code_list_set = list(set(code_list))
    code_list_set.sort(key=code_list.index)
    code_list_str = ",".join(code_list_set)
    return code_list_str

def gen_insert_data(owners_rec_codes_dict):
    owners_data = []
    owners_first_cate = []
    spu_list_str_data = []
    for key, value in owners_rec_codes_dict.items():

        spu_code_dict = owners_rec_codes_dict[key]
        for first_cate in list(spu_code_dict.keys()):
            spu_code_list_str = list_to_str(first_cate, spu_code_dict)
            owners_data.append(key)
            owners_first_cate.append(first_cate)
            spu_list_str_data.append(spu_code_list_str)
    insert_data=list(zip(owners_data, owners_first_cate, spu_list_str_data))
    return insert_data