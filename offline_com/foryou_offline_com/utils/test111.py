import pymysql

ip="127.0.0.1"
userName="root"
password="xmy123456"
db="talk"
port=3306
db = pymysql.connect(ip, userName, password, db, port)
cursor = db.cursor()
# 记录在表中不存在则进行插入，如果存在则进行更新
sql = "INSERT INTO `discover1` VALUES (%s, %s, %s) " \
          "ON DUPLICATE KEY UPDATE `owner_code` = VALUES(`owner_code`), `first_cate` = VALUES(`first_cate`) , spu_code_list = VALUES(`spu_code_list`)"

#数据格式如下：
data_info = (('000005', u'qingdao', 'HZ1111111'),
             ('000005', u'北京123', 'HZ565'),
             ('000008', u'qingdao', 'hhhh'),
             ('000007', u'lllll', 'HZ'))

#批量插入使用executement
cursor.executemany(sql, data_info)
db.commit()
cursor.close()
db.close()