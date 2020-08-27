# @author : zhaizhengyuan
# @datetime : 2020/8/5 16:49
# @file : read_data.py
# @software : PyCharm

"""
读取全部离线计算的基础数据

"""
from foryou_offline_com.utils.database_handle import DatabaseHandle
from foryou_offline_com.utils.configInit import config_dict, get_logger


two_mysql = config_dict
logger = get_logger()
# new_owner_list = ["*2019121112242620370"]


class DataRead(object):
    """
    DataRead类用于读取商品信息
    用户购买数据
    用户收藏数据
    用户评价数据
    """

    def __init__(self):
        """
        方法用于初始化读取的数据库，日志文件等
        """
        self.logger = logger
        self.hisense_mysql = two_mysql["mysql_1"]  # hisense数据库配置
        self.sqyn_mysql = two_mysql["mysql_2"]  # sqyn数据库配置
        self.virtual_area = two_mysql["spring_cloud"]
        self.read_hisense = DatabaseHandle(self.hisense_mysql, logger)
        self.read_sqyn = DatabaseHandle(self.sqyn_mysql, logger)
        self.total_spu_data = None
        self.filter_spu_data = None
        self.filter_service_data = None
        self.buy_spu_data = None
        self.owner_buy_count_data = None
        self.owner_collect_data = None
        self.owner_eval_data = None
        self.goods_cate_data = None
        self.goods_scope_data = None
        self.goods_area_list = None
        self.read_data()

    def read_filter_table(self):
        """
        方法用于读取全部商品信息
        """
        mysqlcmd = '''
            SELECT 
                spu_code, 
                spu_name, 
                sale_price, 
                sale_month_count, 
                first_cate,      
                second_cate                 
            FROM         
                cb_goods_spu_for_filter 
            WHERE 
                spu_name NOT REGEXP "链接|差价"
            ORDER BY
                sale_month_count DESC
         '''
        self.total_spu_data = self.read_sqyn.read_mysql_multipd(mysqlcmd)
        mysqlcmd = '''
            SELECT 
                a.spu_code, 
                a.spu_name, 
                a.sale_price, 
                a.sale_month_count, 
                a.first_cate,      
                a.second_cate                
            FROM         
                cb_goods_spu_for_filter a
            JOIN
                cb_goods_spu_search b
            ON
                a.spu_code = b.spu_code 
            WHERE 
                a.spu_name NOT REGEXP "链接|差价"
            AND
                a.created_time >= "2020-01-01"
            AND
                b.shop_type_code = 1
            OR
                b.shop_type_code = 2
            ORDER BY
                a.sale_month_count DESC
        '''
        self.filter_spu_data = self.read_sqyn.read_mysql_multipd(mysqlcmd)
        mysqlcmd = '''
               SELECT 
                   a.spu_code              
               FROM         
                   cb_goods_spu_for_filter a
               JOIN
                   cb_goods_spu_search b
               ON
                   a.spu_code = b.spu_code 
               WHERE 
                   a.spu_name NOT REGEXP "链接|差价"
               AND
                   a.created_time >= "2020-01-01"
               AND
                   b.shop_type_code = 1
               ORDER BY
                   a.sale_month_count DESC
           '''
        self.filter_service_data = self.read_sqyn.read_mysql_multipd(mysqlcmd)
    # def read_owner_list(self):
    #     """
    #     方法用于读取购买次数大于3的用户列表
    #     """
    #     mysqlcmd = '''
    #         SELECT
    #             owner_code,
    #             COUNT(owner_code) AS COUNT
    #         FROM
    #             cb_owner_buy_goods_info
    #         GROUP BY
    #             owner_code
    #         HAVING
    #             COUNT> 3
    #         ORDER BY COUNT DESC
    #      '''
    #     owner = self.read_sqyn.read_mysql_multipd(mysqlcmd)
    #     self.total_owner_list = owner["owner_code"].tolist()
    #     if self.new_owner_list is []:
    #         self.owner_list = all_owner_list
    #     else:
    #         self.owner_list = self.new_owner_list

    def read_buy_data(self):
        """
        方法用于
        """
        mysqlcmd = '''
            SELECT 
                a.*                   
            FROM         
                cb_owner_buy_goods_info a
            JOIN
                cb_goods_spu_for_filter b
            ON
                a.spu_code = b.spu_code
            WHERE 
                a.spu_name NOT REGEXP "链接|差价"
         '''
        self.buy_spu_data = self.read_sqyn.read_mysql_multipd(mysqlcmd)

    def read_owner_buy_table(self):
        """
        方法用于读取购买次数大于3的用户购买数据
        统计每个用户的购买数量和消费总额
        """
        # mysqlcmd = '''
        #     SELECT
        #         *
        #     FROM
        #         cb_owner_buy_goods_info
        #     WHERE owner_code IN ({})
        #     AND spu_name NOT REGEXP "链接|差价"
        #  '''.format("'%s'" + ",'%s'" * (len(owner_list) - 1)) % tuple(owner_list)
        # self.owner_buy_data = self.read_sqyn.read_mysql_multipd(mysqlcmd)
        mysqlcmd1 = '''
            SELECT 
                owner_code, 
                SUM(sku_count) AS sum1, 
                SUM(real_sku_total) AS sum2 
            FROM 
                cb_owner_buy_goods_info 
            WHERE 
                spu_name NOT REGEXP "链接|差价"
            GROUP BY 
                owner_code
         '''
        self.owner_buy_count_data = self.read_sqyn.read_mysql_multipd(mysqlcmd1)

    def read_owner_collect_table(self):
        """
        方法用于读取用户的历史收藏商品数据
        店铺数据以后添加
        """
        mysqlcmd = '''
            SELECT 
                a.owner_code,
                a.content_code as spu_code,
                b.first_cate,
                b.second_cate
            FROM 
                cb_user_collect_record a
            JOIN
                cb_goods_spu_for_filter b
            ON
                a.content_code = b.spu_code
            WHERE 
                a.collect_type = '01'
            '''
        self.owner_collect_data = self.read_sqyn.read_mysql_multipd(mysqlcmd)

    def read_owner_eval_table(self):
        """
        方法用于读取用户的历史评价数据
        """
        mysqlcmd = '''
            SELECT 
                a.*
            FROM 
                cb_goods_spu_eval a
            JOIN
                cb_goods_spu_for_filter b
            ON
                a.spu_code = b.spu_code
            WHERE 
                a.eval_level = 5
            OR 
                a.eval_level = 4
            '''
        self.owner_eval_data = self.read_sqyn.read_mysql_multipd(mysqlcmd)

    def read_cate_table(self):
        """
        方法用于
        """
        mysqlcmd = '''
                SELECT 
                    cate_code,
                    cate_name,
                    cate_level,
                    cate_parent
                FROM 
                    goods_cate
                '''
        self.goods_cate_data = self.read_hisense.read_mysql_multipd(mysqlcmd)

    def read_goods_scope(self):
        """
        方法用于
        """
        mysqlcmd = '''
                SELECT 
                    a.spu_code,
                    a.area_code
                FROM 
                    cb_goods_scope a
                JOIN
                    cb_goods_spu_for_filter b
                ON
                    a.spu_code = b.spu_code
                '''
        self.goods_scope_data = self.read_sqyn.read_mysql_multipd(mysqlcmd)
        self.goods_area_list = self.goods_scope_data.area_code.tolist()

    def read_data(self):
        """
        方法用于上述读取上述所有数据
        """
        self.read_filter_table()
        self.read_buy_data()
        self.read_owner_buy_table()
        self.read_owner_collect_table()
        self.read_owner_eval_table()
        self.read_cate_table()
        self.read_goods_scope()
