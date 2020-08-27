# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 17:19:20 2019

@author: lijiangman
@amend: zhaizhengyuan
用于进行mysql数据库表的读写操作
"""

import pandas as pd
# from pymysql import connect
import traceback
import pymysql


# 数据库读写操作类
class DatabaseHandle(object):
    """
        数据库读写操作类：
        包括数据库表的读取，
        数据库表中数据的插入，
        数据库表中数据的更新
     """

    def __init__(self, database_dict, logger):
        self._logger = logger
        database_dict['port'] = int(database_dict['port'])
        keys = ['ip', 'user', 'password', 'db', 'port']
        (ip, userName, password, db, port) = (database_dict[i] for i in keys)
        self.database_conn = pymysql.connect(ip, userName, password, db, port)
        # self.database_conn = connect(**database_dict)
        logger.info('数据库连接成功')

    # 数据库表读取操作
    def read_mysql_multipd(self, mysqlcmd):

        data_pd = pd.read_sql(mysqlcmd, self.database_conn)
        return data_pd

    # 数据库表插入数据操作
    # 若为空表，则直接插入，若不为空表则更新
    def insert_table(self, in_sql, up_sql, db_table, data):

        sql = "select count(*) as count from " + db_table
        with self.database_conn.cursor() as cur:
            count = cur.execute(sql)
            if not count:
                self._logger.info('没有查询到数据')
                try:
                    cur.executemany(in_sql, data)
                    self.database_conn.commit()
                except Exception as e:
                    self._logger.error('{}:{}写入数据异常{}'.format(
                        e, db_table, traceback.format_exc()))
                    self.database_conn.rollback()
                    return
            else:
                self._logger.info('查询到{}条数据'.format(count))
                try:
                    cur.executemany(up_sql, data)
                    self.database_conn.commit()
                except Exception as e:
                    self._logger.error('{}:{}写入数据异常{}'.format(
                        e, db_table, traceback.format_exc()))
                    self.database_conn.rollback()
                    return
            cur.close()
            self.database_conn.close()

    # 数据库表的更新操作
    def update_table_data(self, up_sql, data):

        cursor = self.database_conn.cursor()
        cursor.executemany(up_sql, data)
        self.database_conn.commit()
        cursor.close()
        self.database_conn.close()
