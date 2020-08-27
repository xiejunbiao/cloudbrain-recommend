# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 11:10:35 2020
@author: zhaizhengyuan
全量的离线计算与增量的离线计算
用于每天凌晨5点的定时全量更新
以及每隔15（线上30）分钟的增量更新
"""
import multiprocessing
import traceback
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
# from utils.config import ModifyConfFile, log_logger
from hotshop_offline_com.get_shop_list import HotShopRead
from hotshop_offline_com.com_owners_hotshop import OwnerShop, save_owner_shops, add_time
from hotshop_offline_com.find_new_owner import FindNewOwner


# logger = log_logger()
# mcf = ModifyConfFile(logger)
# two_mysql = mcf.return_argv()


class IniAndUpdate(object):
    """
    初始化更新类：
    包含初始化全量计算模块，
    定时增量更新计算模块，
    以及定时任务模块。
    """
    def __init__(self, two_mysql1, logger1):
        # 根据环境修改配置文件,并实例化该类
        self.logger = logger1
        self.two_mysql = two_mysql1
        hsr = HotShopRead(self.two_mysql, self.logger)
        self.shop_list = hsr.sort_shop_list
        self.begin_time = None
        self.end_time = None
        # self.start()

    def full_init(self):
        self.logger.info('-----开始离线初始化全量计算------')
        print("start full_init at {}------------------------".format(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        try:
            os1 = OwnerShop(self.two_mysql, self.logger, self.shop_list, [])
            owner_shop_dict1 = os1.total_owner_score_dict()
            save_data1 = owner_shop_dict1.items()
            save_data = add_time(save_data1)
            save_owner_shops(self.two_mysql, self.logger, save_data)
        except Exception as e:
            self.logger.error('初始化全量计算异常:{}'.format(e), traceback.print_exc())
        else:
            self.logger.info('全量计算完成，数据更新到表cb_hotshop_owner_rec_shops')

    def incre_update(self):
        self.end_time = datetime.datetime.now()
        self.begin_time = self.end_time + datetime.timedelta(minutes=-15)
        self.logger.info('-----开始离线增量计算------')
        print("start incre_update at {}------------------------".format(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        begin_time = self.begin_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            new_owner_list = FindNewOwner(self.two_mysql, self.logger,
                                          begin_time, end_time).new_owner_list
            if new_owner_list:
                os2 = OwnerShop(self.two_mysql, self.logger, self.shop_list, new_owner_list)
                owner_shop_dict2 = os2.total_owner_score_dict()
                save_data2 = owner_shop_dict2.items()
                save_data = add_time(save_data2)
                save_owner_shops(self.two_mysql, self.logger, save_data)
                print("{} new owner is updated------------------------".format(
                    len(new_owner_list)))
            else:
                self.logger.info("no new owner is found")
        except Exception as e:
            self.logger.error('增量计算异常:{}'.format(e), traceback.print_exc())
        else:
            self.logger.info('增量计算完成，数据更新到表cb_hotshop_owner_rec_shops')

    @staticmethod
    def scheduler_task(fun, trigger='interval', hour=5, minute=10):
        scheduler = BlockingScheduler()
        # 采用阻塞的方式
        # 采用固定时间（cron）的方式，每天在固定时间执行
        scheduler.add_job(fun, trigger=trigger, hour=hour, minute=minute)
        # scheduler.add_job(fun, trigger='cron', hour=5, minute=10)
        # scheduler.add_job(self.incre_update(), trigger='interval', minutes=15)
        scheduler.start()

    def task1(self):
        scheduler = BlockingScheduler()
        # 采用阻塞的方式
        # 采用固定时间（cron）的方式，每天在固定时间执行
        scheduler.add_job(self.full_init, trigger='cron', hour=5, minute=20)
        scheduler.start()

    def task2(self):
        scheduler = BlockingScheduler()
        # 采用阻塞的方式
        # 采用固定时间（cron）的方式，每天在固定时间执行
        scheduler.add_job(self.incre_update, trigger='interval', minutes=2)
        scheduler.start()

    # def start(self):
    #     multiprocessing.Process(target=self.task1).start()
    #     multiprocessing.Process(target=self.task2).start()


def start(two_mysql, logger):
    inj = IniAndUpdate(two_mysql, logger)
    inj.full_init()
    multiprocessing.Process(target=inj.task1).start()
    multiprocessing.Process(target=inj.task2).start()


if __name__ == '__main__':
    pass
