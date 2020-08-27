# @author : zhaizhengyuan
# @datetime : 2020/8/13 17:38
# @file : ini_and_update.py
# @software : PyCharm

"""
文件说明：

"""
import datetime
import traceback
import multiprocessing
from apscheduler.schedulers.blocking import BlockingScheduler
from foryou_offline_com.find_new_owner import FindNewOwner
from foryou_offline_com.multi_process import update_work
from foryou_offline_com.multi_process import start_multi_process


class IniAndUpdate(object):
    """
    初始化更新类：
    包含初始化全量计算模块，
    定时增量更新计算模块，
    以及定时任务模块。
    """
    def __init__(self, config, logger):
        # 根据环境修改配置文件,并实例化该类
        self.config = config
        self.logger = logger

    def full_init(self):
        self.logger.info('-----开始离线初始化全量计算------')
        self.logger.info("start full_init foryou and guesslike at {}----------".format(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        try:
            start_multi_process()
        except Exception as e:
            self.logger.error('初始化全量计算异常:{}'.format(e), traceback.print_exc())
        else:
            self.logger.info('全量计算完成，数据更新到cb_foryou,guesslike_owner_rec')

    def incre_update(self):
        self.logger.info('-----开始离线增量计算------')
        self.logger.info("start incre_update foryou and guesslike at {}----------".format(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        try:
            new_owner_list = FindNewOwner(self.config, self.logger).new_owner_list
            if new_owner_list:
                update_work(new_owner_list)
                self.logger.info("{} new owner is updated------------------------".format(
                    len(new_owner_list)))
            else:
                self.logger.info("no new owner is found")
        except Exception as e:
            self.logger.error('增量计算异常:{}'.format(e), traceback.print_exc())
        else:
            self.logger.info('增量计算完成，数据更新到cb_foryou,guesslike_owner_rec')

    @staticmethod
    def scheduler_task(fun, trigger='interval', hour=13, minute=0):
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
        scheduler.add_job(self.full_init, trigger='cron', hour=4, minute=0)
        scheduler.start()

    def task2(self):
        scheduler = BlockingScheduler()
        # 采用阻塞的方式
        # 采用固定时间（cron）的方式，每天在固定时间执行
        scheduler.add_job(self.incre_update, trigger='interval', minutes=15)
        scheduler.start()

    # def start(self):
    #     multiprocessing.Process(target=self.task1).start()
    #     multiprocessing.Process(target=self.task2).start()


def start(two_mysql, logger):
    start_multi_process()
    inj = IniAndUpdate(two_mysql, logger)
    multiprocessing.Process(target=inj.task1).start()
    multiprocessing.Process(target=inj.task2).start()


if __name__ == '__main__':
    pass
