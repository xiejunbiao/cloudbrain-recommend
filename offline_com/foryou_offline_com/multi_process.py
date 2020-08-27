# @author : zhaizhengyuan
# @datetime : 2020/8/13 14:09
# @file : multi_process.py
# @software : PyCharm

"""
文件说明：

"""

import time
import math
import multiprocessing
from foryou_offline_com.utils.database_handle import DatabaseHandle
from foryou_offline_com.utils.configInit import config_dict, get_logger
from foryou_offline_com.profile.goods_prof import GoodsProf
from foryou_offline_com.save_rec_result import SaveRec

two_mysql = config_dict
logger = get_logger()


def get_db_sqyn():
    return DatabaseHandle(two_mysql["mysql_2"], logger)


def div_owner_list():
    """
    方法用于
    """
    mysqlcmd = '''
            SELECT 
                a.owner_code, 
                COUNT(a.owner_code) AS COUNT
            FROM
                cb_owner_buy_goods_info a
            JOIN 
                cb_goods_spu_for_filter b
            ON
                a.spu_code = b.spu_code
            WHERE
                a.spu_name NOT REGEXP "链接|差价" 
            GROUP BY 
                a.owner_code 
            HAVING COUNT> 3
            '''
    owner_data = get_db_sqyn().read_mysql_multipd(mysqlcmd)
    owner_list = owner_data["owner_code"].tolist()

    ave_num = math.floor(len(owner_list) / 5)
    div_list = []
    for i in range(0, len(owner_list), ave_num):
        temp = owner_list[i:i + ave_num]
        div_list.append(temp)
    return div_list


class MultiProcess(object):
    """
    MultiProcess类用于
    """

    def __init__(self, interval, param):
        """
        方法用于
        """
        self.interval = interval
        self.goods = GoodsProf(param)
        self.param = param

    def worker_1(self, div_list1):
        """
        方法用于
        """
        logger.info("begin worker_1")
        SaveRec(div_list1, self.goods, self.param)
        logger.info("end worker_1")
        time.sleep(self.interval)

    def worker_2(self, div_list2):
        """
        方法用于
        """
        logger.info("begin worker_2")
        SaveRec(div_list2, self.goods, self.param)
        logger.info("end worker_2")

    def worker_3(self, div_list3):
        """
        方法用于
        """
        logger.info("begin worker_3")
        SaveRec(div_list3, self.goods, self.param)
        logger.info("end worker_3")

    def worker_4(self, div_list4):
        """
        方法用于
        """
        logger.info("begin worker_4")
        SaveRec(div_list4, self.goods, self.param)
        logger.info("end worker_4")

    def worker_5(self, div_list5):
        """
        方法用于
        """
        logger.info("begin worker_5")
        SaveRec(div_list5, self.goods, self.param)
        logger.info("end worker_5")


def update_work(new_owner_list):
    """
    方法用于
    """
    param = "incre"
    goods = GoodsProf(param)
    SaveRec(new_owner_list, goods, param)


def start_multi_process():
    """
    方法用于
    """
    div_list = div_owner_list()
    param = "full"
    work = MultiProcess(5, param)
    p1 = multiprocessing.Process(target=work.worker_1, args=(div_list[0],))
    p2 = multiprocessing.Process(target=work.worker_2, args=(div_list[1],))
    p3 = multiprocessing.Process(target=work.worker_3, args=(div_list[2],))
    p4 = multiprocessing.Process(target=work.worker_4, args=(div_list[3],))
    p5 = multiprocessing.Process(target=work.worker_5, args=(div_list[4],))
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    # p1.join()
    # p2.join()
    # p3.join()
    # p4.join()
    # p5.join()
