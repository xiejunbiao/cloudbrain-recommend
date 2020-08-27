# @author : zhaizhengyuan
# @datetime : 2020/8/12 9:53
# @file : save_rec_result.py
# @software : PyCharm

"""
文件说明：

"""
import math
import traceback
import numpy as np
import pandas as pd
from foryou_offline_com.offline_com_rec_result import OfflineCom


class SaveRec(OfflineCom):
    """
    SaveRec类用于
    """

    def __init__(self, owner_list, data_pd, param):
        """
        方法用于
        """
        OfflineCom.__init__(self, owner_list, data_pd, param)
        self.logger = data_pd.logger
        self.read_sqyn = data_pd.read_sqyn
        self._save_data()

    def _hotsale_save_data(self):
        """
        方法用于
        """
        save_data_frame = pd.DataFrame(
            columns=("owner_code",
                     "first_cate",
                     "spu_code_list"))
        for key, value in self.owners_hotsale_rec.items():

            first_spu_code_dict = self.owners_hotsale_rec[key]
            for first_cate in list(first_spu_code_dict.keys()):
                spu_code_list_str = ",".join(first_spu_code_dict[first_cate])
                save_data_frame = save_data_frame.append(
                    pd.DataFrame({
                        "owner_code": [key],
                        "first_cate": [first_cate],
                        "spu_code_list": [spu_code_list_str]
                    }))
        data_df = np.array(save_data_frame)
        self.hotsale_data = tuple(data_df.tolist())

    def _foryou_save_data(self):
        """
        方法用于
        """
        owner_area_list = list(self.owners_foryou_rec.keys())
        save_data_frame = pd.DataFrame(
            columns=("owner_area_code",
                     "redis_index",
                     "spu_code_list",
                     "shall_put_redis",
                     "has_next_record",
                     "total_num"))
        # 按照要求，首先对商品列表进行分段，得到分段的索引，
        # 是否将要放入redis,是否有下一条记录等标识
        for owner_area in owner_area_list:
            save_list = self.owners_foryou_rec[owner_area]
            num_of_records = math.floor(len(save_list) / 200)
            # 如果商品列表不满200，默认为是一条数据
            if num_of_records < 1:
                num_of_records = 1
            div_list = []
            # 商品列表分段处理，每段长度为200
            for i in range(0, len(save_list), 200):
                temp = save_list[i:i + 200]
                div_list.append(temp)
            # 最后一段的商品列表如果不足200，合并到上一条记录
            try:
                if len(div_list[num_of_records]) < 200:
                    div_list[num_of_records - 1].extend(div_list[num_of_records])
            except IndexError:
                self.logger.info("The number of key is 200")
            # 根据分段情况，得到各条记录所对应的其他表字段数据
            for i in range(num_of_records):
                try:
                    redis_index = i + 1
                    spu_code_list = div_list[i]
                    spu_code_list_str = ",".join(spu_code_list)
                    if redis_index == 1:
                        shall_put_redis = 1
                    else:
                        shall_put_redis = 0
                    if redis_index < num_of_records:
                        has_next_record = 1
                    else:
                        has_next_record = 0
                    # 每一条数据append到data_frame中
                    save_data_frame = save_data_frame.append(
                        pd.DataFrame({
                            "owner_area_code": [owner_area],
                            "redis_index": [redis_index],
                            "spu_code_list": [spu_code_list_str],
                            "shall_put_redis": [shall_put_redis],
                            "has_next_record": [has_next_record],
                            "total_num": [len(save_list)],
                        }))
                except Exception as e:
                    self.logger.info('计算异常:{}'.format(e), traceback.print_exc())
                    self.logger.info("The rec_spu_list of owner_area is NULL")
        # data_frame转化成能批量写入mysql表的数据
        data_df = np.array(save_data_frame)
        self.foryou_data = tuple(data_df.tolist())

    def _guesslike_save_data(self):
        """
        方法用于
        """
        owner_area_tab_list = list(self.owners_guesslike_rec.keys())
        save_data_frame = pd.DataFrame(
            columns=("owner_area_tab_code",
                     "redis_index",
                     "spu_code_list",
                     "has_next_record",
                     "total_num"))
        # 按照要求，首先对商品列表进行分段，得到分段的索引，
        # 是否将要放入redis,是否有下一条记录等标识
        for owner_area_tab in owner_area_tab_list:
            if owner_area_tab[:3] == "000":
                save_list = self.owners_guesslike_rec[owner_area_tab]
                # num_of_records = len(save_list)
                redis_index = 1
                spu_code_list_str = ",".join(save_list)
                has_next_record = 0
                save_data_frame = save_data_frame.append(
                    pd.DataFrame({
                        "owner_area_tab_code": [owner_area_tab],
                        "redis_index": [redis_index],
                        "spu_code_list": [spu_code_list_str],
                        "has_next_record": [has_next_record],
                        "total_num": [len(save_list)],
                    }))
            else:
                save_list = self.owners_guesslike_rec[owner_area_tab]
                num_of_records = math.floor(len(save_list) / 200)
                # 如果商品列表不满200，默认为是一条数据
                if num_of_records < 1:
                    num_of_records = 1
                div_list = []
                # 商品列表分段处理，每段长度为200
                for i in range(0, len(save_list), 200):
                    temp = save_list[i:i + 200]
                    div_list.append(temp)
                # 最后一段的商品列表如果不足200，合并到上一条记录
                try:
                    if len(div_list[num_of_records]) < 200:
                        div_list[num_of_records - 1].extend(div_list[num_of_records])
                except IndexError:
                    self.logger.info("The number of key is 200")
                # 根据分段情况，得到各条记录所对应的其他表字段数据
                for i in range(num_of_records):
                    try:
                        redis_index = i + 1
                        spu_code_list = div_list[i]
                        spu_code_list_str = ",".join(spu_code_list)
                        if redis_index < num_of_records:
                            has_next_record = 1
                        else:
                            has_next_record = 0
                        # 每一条数据append到data_frame中
                        save_data_frame = save_data_frame.append(
                            pd.DataFrame({
                                "owner_area_tab_code": [owner_area_tab],
                                "redis_index": [redis_index],
                                "spu_code_list": [spu_code_list_str],
                                "has_next_record": [has_next_record],
                                "total_num": [len(save_list)],
                            }))
                    except Exception as e:
                        self.logger.info('计算异常:{}'.format(e), traceback.print_exc())
                        self.logger.info("The rec_spu_list of owner_area_tab is NULL")
        # data_frame转化成能批量写入mysql表的数据
        data_df = np.array(save_data_frame)
        self.guesslike_data = tuple(data_df.tolist())

    def _hotsale_data_to_mysql(self):
        """
        方法用于
        """
        temp_list = list(self.owners_hotsale_rec.keys())
        temp_list = [temp_list[i][1:] for i in range(len(temp_list))]
        del_sql = """
            DELETE from
                cb_hotsale_owner_rec_goods
            where
                owner_code
            REGEXP "{}"
        """.format("%s" + "|%s" * (len(temp_list) - 1)) % tuple(temp_list)
        in_sql = """
            INSERT INTO cb_hotsale_owner_rec_goods (
                owner_code,
                first_cate,
                spu_code_list
            ) 
            VALUES({})
        """.format('%s' + ', %s' * 2)
        # up_sql = """INSERT INTO cb_hotsale_owner_rec_goods
        #             VALUES (0, {})
        #             ON DUPLICATE KEY UPDATE
        #             `owner_code` = VALUES(`owner_code`),
        #             `first_cate` = VALUES(`first_cate`) ,
        #             `spu_code_list` = VALUES(`spu_code_list`)""".format('%s' + ', %s' * 2)
        table = "cb_hotsale_owner_rec_goods"
        self.read_sqyn.del_insert_data(del_sql, in_sql, self.hotsale_data, table)

    def _foryou_data_to_mysql(self):
        """
        方法用于
        """
        temp_list = list(self.owners_foryou_rec.keys())
        temp_list = [temp_list[i][1:] for i in range(len(temp_list))]
        del_sql = """
            DELETE from 
                cb_foryou_owner_rec_goods 
            where 
                owner_area_code 
            REGEXP "{}"
        """.format("%s" + "|%s" * (len(temp_list) - 1)) % tuple(temp_list)
        in_sql = """
            INSERT INTO cb_foryou_owner_rec_goods (
                owner_area_code,
                redis_index,
                spu_code_list,
                shall_put_redis,
                has_next_record,
                total_num
            ) 
            VALUES({})
        """.format('%s' + ', %s' * 5)
        table = "cb_foryou_owner_rec_goods"
        self.read_sqyn.del_insert_data(del_sql, in_sql, self.foryou_data, table)

    def _guesslike_data_to_mysql(self):
        """
        方法用于
        """
        temp_list = list(self.owners_guesslike_rec.keys())
        temp_list = [temp_list[i][1:] for i in range(len(temp_list))]

        del_sql = """
            DELETE from 
                cb_guesslike_owner_rec_goods 
            where 
                owner_area_tab_code 
             REGEXP "{}"
        """.format("%s" + "|%s" * (len(temp_list) - 1)) % tuple(temp_list)
        in_sql = """
            INSERT INTO cb_guesslike_owner_rec_goods (
                owner_area_tab_code,
                redis_index,
                spu_code_list,
                has_next_record,
                total_num
            ) 
            VALUES({})
        """.format('%s' + ', %s' * 4)
        table = "cb_guesslike_owner_rec_goods"
        self.read_sqyn.del_insert_data(del_sql, in_sql, self.guesslike_data, table)

    def _save_data(self):
        """
        方法用于
        """
        self._foryou_save_data()
        self._foryou_data_to_mysql()

        # self._hotsale_save_data()
        # self._hotsale_data_to_mysql()

        self._guesslike_save_data()
        self._guesslike_data_to_mysql()


if __name__ == "__main__":
    pass

