# -*- coding: utf-8 -*-
"""
Create Time: 2020/8/5 14:50
Author: xiejunbiao
"""
from pymysql.cursors import DictCursor
from online_server.utils.tool_kit import get_mysql_conn


class GetDataFromDatabase(object):
    def __init__(self, service_code_tuple=None, txt_list=None, dict_=False, db='sqyn'):
        self.db = db
        if dict_:
            self.conn_sqyn = get_mysql_conn('mysql_2')
            self.cur_sqyn = self.conn_sqyn.cursor(cursor=DictCursor)
            self.conn_hisense = get_mysql_conn('mysql_1')
            self.cur_hisense = self.conn_hisense.cursor(cursor=DictCursor)
        else:
            self.conn_sqyn = get_mysql_conn('mysql_2')
            self.cur_sqyn = self.conn_sqyn.cursor()
            self.conn_hisense = get_mysql_conn('mysql_1')
            self.cur_hisense = self.conn_hisense.cursor()

        tmp_txt = ''
        if txt_list:
            for txt in txt_list:
                tmp_txt = tmp_txt + '|' + txt

        self._sql_select_spu_all = """
                           SELECT 
                                    b.area_code, 
                                    c.shop_code, 
                                    a.spu_code, 
                                    a.sale_month_count
                               FROM cb_goods_spu_for_filter a 
                                    JOIN cb_goods_scope b ON a.spu_code=b.spu_code 
                                    JOIN cb_goods_spu_search c ON a.spu_code=c.spu_code 
                               WHERE a.spu_name NOT REGEXP "{}"
                                    AND a.price_line > a.sale_price
                                    AND c.shop_type_code = 1
                                    AND a.second_cate NOT IN {}
                           """.format(tmp_txt[1:], service_code_tuple)

        self._sql_select_coupon_shop = """
                                       SELECT area_code, shop_code, coupon_type 
                                        FROM cb_shop_coupon 
                                        WHERE status = '02' AND coupon_type='02' OR coupon_type='03'
                                        GROUP BY area_code, shop_code, coupon_type
                                       """

        #
        self._sql_select_promotion = """
                                     SELECT act_code, 
                                            end_time, 
                                            begin_time, 
                                            shop_code, 
                                            area_code, 
                                            spu_code 
                                     FROM   cb_goods_promotion
                                     WHERE 	CURRENT_TIMESTAMP BETWEEN  
                                                DATE_FORMAT(begin_time, '%Y-%m-%d %H:%i:%s') 
                                            AND
                                                DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s');
                                     """

        self._sql_select_price_line = """
                                      SELECT b.area_code, a.spu_code 
                                      FROM  cb_goods_spu_for_filter a 
                                            JOIN cb_goods_scope b ON a.spu_code=b.spu_code 
                                      WHERE a.price_line > a.sale_price
                                      """

        self._sql_select_discount_hot_sale = """
                                            SELECT  
                                      FROM  cb_discount_goods_rec_results a 
                                            JOIN cb_goods_scope b ON a.spu_code=b.spu_code 
                                      WHERE area_code='{}'
                                      """

        self._sql_select_spu_all_new_goods = """
                                   SELECT   	b.area_code,
                                                a.spu_code,
                                                a.sale_month_count,
                                                a.created_time,
                                                a.first_cate,
                                                a.second_cate
                                            FROM        cb_goods_spu_for_filter AS a
                                            JOIN    cb_goods_scope AS b ON a.spu_code=b.spu_code
                                            JOIN cb_goods_spu_search c ON a.spu_code=c.spu_code
                                            WHERE   a.sale_price > 10
                                                AND a.spu_name NOT REGEXP "{}"
                                                AND a.second_cate NOT IN {}
                                                AND c.shop_type_code = 1
                                                AND TO_DAYS(NOW()) - TO_DAYS(a.created_time) <= 30
                                            GROUP BY b.area_code,spu_code
                                            ORDER BY a.created_time DESC
                                   """.format(tmp_txt[1:], service_code_tuple)

        self._sql_select_all_area = """
                                    select area_code from cb_goods_scope group by area_code
                                    """

    def get_all_area(self):
        return self._get_data(self._sql_select_all_area)

    # 获得新品好货所有spu数据
    def get_data_spu_all_new_goods(self):
        return self._get_data(self._sql_select_spu_all_new_goods)

    def get_data_discount_hot_sale(self):
        return self._get_data(self._sql_select_discount_hot_sale)

    def get_data_spu_all(self):
        return self._get_data(self._sql_select_spu_all)

    def get_data_coupon_shop(self):
        return self._get_data(self._sql_select_coupon_shop)

    def get_data_promotion(self):
        return self._get_data(self._sql_select_promotion)

    def get_data_spu_price_line(self):
        return self._get_data(self._sql_select_price_line)

    def _get_data(self, sql_select):
        data = None
        try:
            self.conn_sqyn = get_mysql_conn('mysql_2')
            self.cur_sqyn = self.conn_sqyn.cursor(cursor=DictCursor)
            self.cur_sqyn.execute(sql_select)
            data = self.cur_sqyn.fetchall()
            self.cur_sqyn.close()
            self.conn_sqyn.close()
        except Exception as e:
            print(e)
        return data
