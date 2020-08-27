# @author : zhaizhengyuan
# @datetime : 2020/8/13 15:30
# @file : find_new_owner.py
# @software : PyCharm

"""
文件说明：

"""
from foryou_offline_com.utils.database_handle import DatabaseHandle
# from offline_com.foryou_offline_com.utils.configInit import config_dict, log_logger


# two_mysql = config_dict
# logger = log_logger()


class FindNewOwner(object):
    """
    FindNewOwner类用于
    """

    def __init__(self, two_mysql, logger):
        """
        方法用于
        """

        self.read_sqyn = DatabaseHandle(two_mysql["mysql_2"], logger)
        self.new_order_owner = []
        self.new_collect_owner = []
        self.new_eval_owner = []
        self.new_owner_list = []
        self.total_new_owner()

    def find_new_order_owner(self):
        """
        方法用于
        """
        mysql = '''
            SELECT 
                owner_code 
            FROM 
                cb_owner_buy_goods_info 
            WHERE 
                paid_time >= CURRENT_TIMESTAMP - INTERVAL 30 MINUTE
            '''
        order_data = self.read_sqyn.read_mysql_multipd(mysql)
        if len(order_data) != 0:
            self.new_order_owner = order_data["owner_code"].tolist()

    def find_new_collect_owner(self):
        """
        方法用于
        """
        mysql = ''' 
            SELECT 
                owner_code 
            FROM 
                cb_user_collect_record 
            WHERE 
                created_time  >= CURRENT_TIMESTAMP - INTERVAL 30 MINUTE
            AND
                collect_type = '01'
            '''
        # BETWEEN
        #     '%s'
        # AND
        #     '%s'
        # ''' % (self.begin_time, self.end_time)
        collect_data = self.read_sqyn.read_mysql_multipd(mysql)
        if len(collect_data) != 0:
            self.new_collect_owner = collect_data["owner_code"].tolist()

    def find_new_eval_owner(self):
        """
        方法用于
        """
        mysql = '''
                SELECT 
                    owner_code 
                FROM 
                    cb_goods_spu_eval 
                WHERE 
                    eval_time >= CURRENT_TIMESTAMP - INTERVAL 30 MINUTE
                '''
        eval_data = self.read_sqyn.read_mysql_multipd(mysql)
        if len(eval_data) != 0:
            self.new_eval_owner = eval_data["owner_code"].tolist()

    def total_new_owner(self):
        """
        方法用于
        """
        self.find_new_order_owner()
        self.find_new_collect_owner()
        self.find_new_eval_owner()
        total_new_owner_list = self.new_order_owner + self.new_collect_owner + \
            self.new_eval_owner
        self.new_owner_list = list(set(total_new_owner_list)) + ["000"]
