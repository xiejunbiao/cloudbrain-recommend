# -*- coding: utf-8 -*-
"""
Create Time: 2020/8/3 14:00
Author: xiejunbiao
"""
# from pymysql import connect
import operator
import random
# from online_server.utils.configInit import config_dict
# from apscheduler.schedulers.blocking import BlockingScheduler


def updata_base_data():
    print(1)


# def task():
#     scheduler = BlockingScheduler()
#     scheduler.add_job(updata_base_data, trigger='cron', hour=14, minute=7)
#     scheduler.start()
#
#
# def mysql_list():
#     cur = connect(config_dict['mysql_sqyn']).cursor()
#     cur.execute('select * from cb_goods_scope')
#     data = cur.fetchall()
#     print(data)


def compare():
    tuple1, tuple2 = (123, 'xyz'), (456, 'abc')
    tuple3 = [(123,)]
    tuple4 = (123,)

    print(operator.eq(tuple1, tuple2))
    print(operator.eq(tuple3, tuple4))
    print(operator.contains(tuple3, tuple4))
    import pandas as pd
    all_data = pd.read_csv("E:/协和问答系统/SenLiu/熵测试数据.csv")
    # 获取某一列值为xx的行的候选列数据
    print(all_data)
    feature_data = all_data.iloc[:, [0, -1]][all_data[all_data.T.index[0]] == '青年']


import pandas as pd




def t1():
    a = [{'a': 22, 'b': 22},
         {'a': 34, 'b': 33},
         {'a': 21, 'b': 33},
         {'a': 21, 'b': 36},
         {'a': 21, 'b': 39}]
    data = pd.DataFrame(a)
    print(data[~data['b'].isin([33, 22])])
    print(list(data[~data['b'].isin([33, 22])].groupby('a').size().index))


def t2():
    a = [{'a': 22, 'b': 22},
         {'a': 34, 'b': 33},
         {'a': 21, 'b': 33},
         {'a': 21, 'b': 36},
         {'a': 21, 'b': 39},
         {'a': 22, 'b': 22},
         {'a': 34, 'b': 33},
         {'a': 20, 'b': 33},
         {'a': 20, 'b': 36},
         {'a': 21, 'b': 3}
         ]

    b = [{'a': 22, 'b': 22},
         {'a': 34, 'b': 33},
         {'a': 21, 'b': 33},
         {'a': 21, 'b': 36},
         {'a': 21, 'b': 3}]
    a = pd.DataFrame(a).groupby('a').get_group(21)
    b = pd.DataFrame(b)
    # a.append(b, ignore_index=False, verify_integrity=False, sort=None)
    # print(a)
    c = pd.concat([a, b], ignore_index=False)
    d = c.reset_index(drop=True).sort_values(by=['a', 'b'], ascending=[False, False])
    print(d)


def t3():
    a = [{'a': 22, 'b': 22},
         {'a': 34, 'b': 33},
         {'a': 21, 'b': 33},
         {'a': 21, 'b': 36},
         {'a': 21, 'b': 39},
         {'a': 22, 'b': 22},
         {'a': 34, 'b': 33},
         {'a': 20, 'b': 33},
         {'a': 20, 'b': 36},
         {'a': 20, 'b': 33},
         {'a': 20, 'b': 32},
         {'a': 20, 'b': 38},
         {'a': 21, 'b': 3}
         ]
    # a = []
    a = pd.DataFrame(a)
    data = a.drop_duplicates('a')
    print(data)
        # .apply(lambda x: x.sort_values('b', ascending=False))
    # print(data)
    # print(list(data.size().index))
    # print(data.get_group(20).sort_values('b', ascending=False)['a'])


def t4():
    a_list = []
    # if len(a_list) < 2:
    #     return
    print(random.sample(a_list, 2))


def t5():
    mailto = ['cc', 'bbbb', 'afa', 'sss', 'bbbb', 'cc', 'shafa']
    addr_to = list(set(mailto))
    addr_to.sort(key=mailto.index)
    print(addr_to)

def t6():
    import numpy as np
    df = pd.DataFrame(np.random.randn(8, 4), index=['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'], columns=['A', 'B', 'C', 'D'])
    df.to_csv('21.csv')
    df = pd.read_csv('21.csv', index_col=False)
    print(df)
    # print(df.loc[['a'], ['A']])


if __name__ == '__main__':
    # task()
    # mysql_list()
    # compare()
    t6()
