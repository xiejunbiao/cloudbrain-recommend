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
    def insert_table(self, in_sql, up_sql, data, db_table):

        sql = "select count(*) as count from " + db_table
        self.database_conn.ping(reconnect=True)
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

    def del_insert_data(self, del_sql, insert_sql, data, table):
        """
        方法用于
        """
        self.database_conn.ping(reconnect=True)
        cursor = self.database_conn.cursor()
        cursor.execute(del_sql)
        try:
            cursor.executemany(insert_sql, data)
            self.database_conn.commit()
        except Exception as e:
            self._logger.error('{}:{}写入数据异常{}'.format(
                e, table, traceback.format_exc()))
            self.database_conn.rollback()
            return
        cursor.close()
        self.database_conn.close()

    # 数据库表的更新操作
    def update_table_data(self, up_sql, data):

        cursor = self.database_conn.cursor()
        cursor.executemany(up_sql, data)
        self.database_conn.commit()
        cursor.close()
        self.database_conn.close()

# # -*- coding: utf-8 -*-
# """
# Created on Tue Oct 15 17:19:20 2019
#
# @author: lijiangman
# """
# #数据库读取类
# # python2  import MySQLdb
# import pymysql# python3
# import pandas as pd
# #from Owners_RecV2.utils.configInit import config_result
#
# # ##数据库配置
# # def databaseSet():
# #     #######数据库配置文件#########
# #     #envIp='testxwj.juhaolian.cn'##青岛准生产，测试环境--这个用https
# #     # envIp='xwj.juhaolian.cn'##北京正式环境，https
# #     #
# #     # ip = ["10.18.226.38","10.18.226.58"]
# #     # db = ["hisense","hisense_bi_data_rec"]
# #     # userName = ["root","root"]
# #     # password = ["123456","Hisense,123"]
# #     # port = [3306,3100]
# #     ##本地数据库自己配置，测试或线上环境根据配置文件
# #     our = ["10.18.226.58","hisense_bi_data_rec","root","Hisense,123",3100]
# #     #dbTable = ['owner_spus_for_hotsale','goods_spu_for_filter']
# #
# #     database_dict = {}
# #     database_dict0 = {'ip':config_result['ip'],'db':config_result['db'],'userName':config_result['user'],\
# #                     'password':config_result['password'],'port':int(config_result['port'])}
# #     database_dict1 = {'ip':our[0],'db':our[1],'userName':our[2],\
# #                       'password':our[3],'port': our[4]}
# #     database_dict['test_database'] = database_dict0
# #     database_dict['our_database'] = database_dict1
# #     return database_dict
#
# # def connect_db(database):
# #
# #     dbc = pymysql.connect(host=config_result[database]['ip']
# #                          ,user=config_result[database]['user']
# #                          ,password=config_result[database]['password']
#     #                      ,database=config_result[database]['db']
#     #                      ,port=config_result[database]['port'])
#     # return dbc
#
# def connect_db(database_dict):
#      #databases_dict = databaseSet()
#      #database_dict = databases_dict[database]
#      keys = ['ip', 'user', 'password', 'db', 'port']
#      #print(int(database_dict['port']))
#      database_dict['port'] = int(database_dict['port'])
#      #print(database_dict)
#      (ip, userName, password, db, port) = (database_dict[i] for i in keys)
#      db = pymysql.connect(ip, userName, password, db, port)
#      return db
# #############################
# def readMysqlmultiPd(mysqlCmd,database):
#
#     db = connect_db(database)
#     dataPd=pd.read_sql(mysqlCmd,db)
#     db.close()
#     return dataPd
#
#
# def insert_table(in_sql, del_sql, data, database):
#
#     db = connect_db(database)
#     cursor = db.cursor()
#     cursor.execute(del_sql)
#     # sql = "select count(*) from " + dbTable
#     # dataPd = pd.read_sql(sql, db)
#     # if len(dataPd)==0:
#     #    cursor.executemany(in_sql, data)
#     # else:
#     #    cursor.executemany(up_sql, data)
#     cursor.executemany(in_sql, data)
#     db.commit()
#     cursor.close()
#     db.close()
#
# def update_table_data(upsql,data,database):
#
#     db = connect_db(database)
#     cursor = db.cursor()
#     try:
#         cursor.executemany(upsql,data)
#         db.commit()
#     except Exception as e:
#         ######错误回滚
#         db.rollback()
#     cursor.close()
#     db.close()
# # def create_insert_table(cmysqlCmd,imysqlCmd,insert_data,database):
# #
# #     db =connect_db(database)
# #     cursor = db.cursor()
# #     cursor.execute(cmysqlCmd)
# #     print("table is created")
# #     cursor.executemany(imysqlCmd, insert_data)
# #     db.commit()
# #     cursor.close()
# #     db.close()
#
# # def update_table(table,crsql,insql,upsql,data,database):
# #
# #     db = connect_db(database)
# #     cursor = db.cursor()
# #     sql = "show tables"
# #     cursor.execute(sql)
# #     tables = cursor.fetchall()
# #     tables_list = re.findall('(\'.*?\')', str(tables))
# #     tables_list = [re.sub("'", '', each) for each in tables_list]
# #     if table not in tables_list:
# #         cursor.execute(crsql)
# #         cursor.executemany(insql, data)
# #         db.commit()
# #         cursor.close()
# #         db.close()
# #         print("%s table is created and initialized" % table)
# #     else:
# #         cursor.executemany(upsql, data)
# #         db.commit()
# #         cursor.close()
# #         db.close()
# #         print("%s table is exist and updated" % table)
#
# # def readGood_areas(areaCode,cateCode):
# #     # filterSql = ""
# #     ##小区代码
# #     #    dbTable2="goods_scope"##小区代码和spu代码对应关系
# #     # if areaCode == '-1':  ##没有shopcode
# #     #     #filterSql = " where a.spu_code=b.spu_code and a.spu_cate_first = c.cate_name" +\
# #     #     #            " and d.spu_code=a.spu_code and e.sku_no=d.sku_no and e.shop_code=a.shop_code"+\
# #     #     #            " and a.goods_status=1 and e.store_number >0"  # 过滤sql语句---#在线商品1
# #     #     filterSql = " where a.spu_code=b.spu_code and a.spu_cate_first = c.cate_name" + \
# #     #                 " and a.goods_status=1"  # 过滤sql语句---#在线商品1
# #
# #     # else:
# #     #     filterSql = " where a.goods_status=1 " + " and a.spu_code in (select spu_code from " + dbTable2 + " where area_code=" + areaCode + ")" +" and a.spu_cate_first = c.cate_name"
# #     #     #print(filterSql)
# #
# #     # if cateCode == '-1':
# #     #     filterSql = filterSql
# #     # else:
# #     #     filterSql = filterSql+" and c.cate_code=" + cateCode
# #
# #
# #     mysqlCmd="SELECT a.* FROM goods_spu_search a\
# #  JOIN goods_scope b ON a.spu_code=b.spu_code\
# #  AND b.area_code='%s'\
# #  AND a.goods_status=1\
# #  JOIN goods_spu c ON a.spu_code=c.spu_code\
# #  AND c.cate_code IN (SELECT cate_code FROM goods_cate\
# #  WHERE cate_parent IN (SELECT cate_code FROM goods_cate\
# #  WHERE cate_parent='%s'))" % (areaCode,cateCode)
# #
# #     #print(mysqlCmd)
# #
# #     dataDf=readMysqlmultiPd(mysqlCmd)
# #     # dataDf = readMysqlmultiPd(ip, userName, password, db, dbTable1, dbTable2, dbTable3, mysqlCmd)
# #     return dataDf
#
# # def filt_spu_codes_list(dbTable,dbTable1,ownerCode,cateCode):
# #
# #     ##小区代码
# #     #    dbTable2="goods_scope"##小区代码和spu代码对应关系
# #     filterSql = " where owner_code='%s' and first_cate='%s'" %(ownerCode,cateCode)
# #     db = pymysql.connect(ip,userName,password,db,port=3100)
# #     cursor = db.cursor()
# #     mysqlCmd = "SELECT spu_code_list FROM " + dbTable + filterSql
# #     dataPd = pd.read_sql(mysqlCmd,db)
# #     spu_code_list_str =dataPd['spu_code_list'][0]
# #     filterSql1 = " where goods_status =1 and store_status=1 and spu_code in (" + spu_code_list_str + ")"
# #     mysqlCmd = "SELECT * FROM " + dbTable1 + filterSql1
# #     dataPd = pd.read_sql(mysqlCmd,db)
# #     db.close()
# #     return dataPd
#
# # def create_area_spu_cate_table(ip,userName,password,db,dbTable, spu_names, spu_codes, area_codes, cate_codes, sales_counts,sales_prices,shop_codes):
# #     db = pymysql.connect(ip, userName, password, db)
# #     cursor = db.cursor()
# #     sql5 = '''drop table  if exists ''' + dbTable
# #     cursor.execute(sql5)
# #     # sql='''create table '''+dbTable+''' (spu_code char(100), spu_name char(100), spu_g_name char(100))'''
#
# #     # id自增
# #     sql = '''create table ''' + dbTable + ''' (good_id int PRIMARY key auto_increment,\
# #      spu_code char(100), spu_name char(100), area_code char(100), cate_code char(100),\
# #       sales_count int, sales_price float, shop_code char(100))'''
# #     cursor.execute(sql)
#
# #     spu_len = len(spu_codes)
# #     for i in range(spu_len):
# #         spu_name = spu_names[i]
# #         spu_code = spu_codes[i]
# #         area_code = area_codes[i]
# #         cate_code = cate_codes[i]
# #         cate_code = str(cate_code)
# #         sale_count = sales_counts[i]
# #         sale_price = sales_prices[i]
# #         shop_code = shop_codes[i]
# #         sql1 = 'INSERT INTO ' + dbTable + "(spu_code,spu_name,area_code,cate_code,sales_count,sales_price,shop_code) VALUES ('%s', '%s', '%s', '%s', '%d','%0.2f','%s')" %\
# #                                           (spu_code,spu_name,area_code,cate_code,sale_count,sale_price,shop_code)
# #         cursor.execute(sql1)
# #     db.commit()
# #     cursor.close()
# #     db.close()
#
#
# # def read_good_first_catelist(ip, dbTable,db, userName, password,cateCode):
# #     # 打开数据库连接
# #     db1 = pymysql.connect(ip, userName, password, db)
# #     # 使用 cursor() 方法创建一个游标对象 cursor
# #     cursor = db1.cursor()
#
# #     mysqlCmd = "select cate_code from "+dbTable+" where cate_parent="+cateCode
#
# #     cursor.execute(mysqlCmd)
# #     data = cursor.fetchall()
# #     catelist = []
# #     catelist.append(cateCode)
# #     for i in range(len(data)):
# #         catelist.append(str(data[i][0]))
# #         mysqlCmd = "select cate_code from " + dbTable + " where cate_parent=" + str(data[i][0])
# #         cursor.execute(mysqlCmd)
# #         data1 = cursor.fetchall()
# #         for j in range(len(data1)):
# #             catelist.append(str(data1[j][0]))
# #     catelist = list(set(catelist))
# #     return catelist
#
#
#
# # def read_good_first_cate(ip, dbTable,db, userName, password,cateCode):
# #     # 打开数据库连接
# #     db1 = pymysql.connect(ip, userName, password, db)
# #     # 使用 cursor() 方法创建一个游标对象 cursor
# #     cursor = db1.cursor()
#
# #     mysqlCmd = "select cate_level,cate_parent from "+dbTable+" where cate_code="+cateCode
#
# #     cursor.execute(mysqlCmd)
# #     data = cursor.fetchone()
# #     dataPd = data[0]
# #     cateparent = data[1]
# #     if dataPd ==1:
# #         cate_code = cateCode
# #     elif dataPd ==2:
# #         mysqlCmd = "select cate_parent from " + dbTable + " where cate_code=" + cateCode
# #         cursor.execute(mysqlCmd)
# #         data = cursor.fetchone()
# #         cate_code = data[0]
# #     else:
# #         cateCode = str(cateparent)
# #         mysqlCmd = "select cate_parent from " + dbTable + " where cate_code=" + cateCode
# #         cursor.execute(mysqlCmd)
# #         data = cursor.fetchone()
# #         cate_code = data[0]
# #     return cate_code
#
#
# # def mysql_excute(ip,username,password,db,excutesql):
#
# # ##创建connection连接
# #       conn = pymysql.connect(ip,username,password,db) #db1为数据库名称
# #       cur = conn.cursor()
# #       cur.excute(excutesql)
# #       conn.commit()
# #       cur.close()
# #       conn.close()
#
#
# # def update_area_spu_cate_table(ip, userName, password,db,dbTable, spu_names, spu_codes, area_codes, cate_codes, sales_counts,sales_prices,shop_codes):
#
# #     # 打开数据库连接
# #     db = pymysql.connect(ip, userName, password, db)
# #     # 使用 cursor() 方法创建一个游标对象 cursor
# #     cursor = db.cursor()
# #     searchsql = "select * from " +dbTable
# #     dataPd = pd.read_sql(searchsql, db)
# #     ##字段描述
# #     mysqlCmd = "desc " + dbTable
# #     cursor.execute(mysqlCmd)
# #     spu_codeslist = dataPd['spu_code']
# #     area_codeslist = dataPd['area_code']
# #     shop_codeslist = dataPd['shop_code']
# #     db.commit()
# #     cursor.close()
# #     db.close()
# #     ##首先反向查看旧表中是否存在新表中不存在的商品代码，如果存在则首先要删除这些商品信息
# #     for i in range(len(spu_codeslist)):
# #         if spu_codeslist[i] not in spu_codes:
# #             deletesql = "delete from "+dbTable+" where spu_code="+spu_codeslist[i]
# #             mysql_excute(ip,userName,password,db,deletesql)
# #     ##其次，根据新表中的商品数据对旧表中的数据进行更新
# #     for i in range(len(spu_codes)):
# #         if spu_codes[i] in spu_codeslist:
# #             loc=spu_codeslist.index(spu_codes[i])
# #             area_code = area_codeslist[loc]
# #             shop_code = shop_codeslist[loc]
# #             if area_code in area_codes[i] and shop_code in shop_codes[i]:
# #                 updatesql="update "+dbTable+" set cate_code="+cate_codes[i]+\
# #                           ", sales_count="+sales_counts[i]+", sales_price="+sales_prices[i]+\
# #                           " where spu_code="+spu_codes[i]+" and area_code="+area_codes[i]+" and shop_code="+shop_codes[i]
# #                 mysql_excute(ip,userName,password,db,updatesql)
# #             else:##如果不存在，说明此小区以及店铺不存在该商品，需要将其删除
# #                 deletesql = "delete from "+dbTable+" where spu_code="+spu_codeslist[loc]+\
# #                     " and area_code="+area_codeslist[loc]+" and shop_code="+shop_codeslist[loc]
# #                 mysql_excute(ip,userName,password,db,deletesql)
# #         else:##这是新增加的商品数据，需要将其插入到新表中
# #             insertsql="INSERT INTO " + dbTable + "(spu_code,spu_name,area_code,cate_code,sales_count,sales_price,shop_code) VALUES ('%s', '%s', '%s', '%s', '%d','%0.2f','%s')" %\
# #                                           (spu_codes[i],spu_names[i],area_codes[i],cate_codes[i],sales_counts[i],sales_prices[i],shop_codes[i])
# #             mysql_excute(ip,userName,password,db,insertsql)
#
