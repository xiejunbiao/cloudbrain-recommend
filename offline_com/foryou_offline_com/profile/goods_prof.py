# @author : zhaizhengyuan
# @datetime : 2020/8/6 10:39
# @file : goods_prof.py
# @software : PyCharm

"""
文件说明：计算商品画像

"""
import numpy as np
from foryou_offline_com.profile.read_data import DataRead
from foryou_offline_com.utils.basic_handles import sort_adict
from foryou_offline_com.utils.basic_handles import get_match_score
from foryou_offline_com.utils.basic_handles import keywords_extra


class GoodsProf(DataRead):
    """
    GoodsProf类用于
    """

    def __init__(self, param):
        """
        方法用于
        """
        DataRead.__init__(self)
        self.all_spu_name_dict = {}
        self.first_second_spu = {}
        self.tab_spu_code = {}
        self._total_goods_prof(param)

    def _goods_list(self):
        """
        方法用于
        """
        self.all_spu_list = self.total_spu_data["spu_code"].tolist()
        self.filter_spu_list = self.filter_spu_data["spu_code"].tolist()
        self.filter_service_list = self.filter_service_data["spu_code"].tolist()
        self.buy_spu_list = self.buy_spu_data["spu_code"].tolist()
        self.collect_spu_list = self.owner_collect_data["spu_code"].tolist()
        self.eval_spu_list = self.owner_eval_data["spu_code"].tolist()
        self.owners_spu_codes_list = list(set(self.buy_spu_list +
                                              self.collect_spu_list +
                                              self.eval_spu_list) & set(self.all_spu_list))
        self._first_cate_list = list(set(self.filter_spu_data["first_cate"].tolist()))
        self._secon_cate_list = list(set(self.filter_spu_data["second_cate"].tolist()))

    # def _goods_sort(self):
    #     """
    #     方法用于
    #     """
    #     buy_count = [len(self.buy_spu_data[
    #                          self.buy_spu_data.spu_code == spu])
    #                  for spu in self.all_spu_list]
    #     collect_count = [len(self.owner_collect_data[
    #                          self.owner_collect_data.spu_code == spu])
    #                      for spu in self.all_spu_list]
    #     eval_count = [len(self.owner_eval_data[
    #                          self.owner_eval_data.spu_code == spu])
    #                   for spu in self.all_spu_list]
    #     all_spu_count_list = [0.3 * buy_count[i] +
    #                           0.5 * collect_count[i] +
    #                           0.2 * eval_count[i]
    #                           for i in range(0, len(self.all_spu_list))]
    #     self._goods_count_dict = dict(zip(self.all_spu_list, all_spu_count_list))
    #     self._goods_count_dict = sort_adict(self._goods_count_dict)
    #     self.sorted_all_spu_list = list(self._goods_count_dict.keys())

    def _goods_name(self):
        """
        方法用于
        """
        for spu_code in self.all_spu_list:
            spu_name = self.total_spu_data[
                self.total_spu_data.spu_code == spu_code].spu_name.values[0]
            spu_keywords = keywords_extra(spu_name)
            self.all_spu_name_dict[spu_code] = spu_keywords

    def _goods_cate(self):
        """
        方法用于
        """

        first_count = [len(self.buy_spu_data[
                             self.buy_spu_data.first_cate == cate])
                       for cate in self._first_cate_list]
        secon_count = [len(self.buy_spu_data[
                             self.buy_spu_data.second_cate == cate])
                       for cate in self._secon_cate_list]
        self.first_count_dict = dict(zip(self._first_cate_list, first_count))
        self.first_count_dict = sort_adict(self.first_count_dict)
        self.first_cate_list = list(self.first_count_dict.keys())
        self.secon_count_dict = dict(zip(self._secon_cate_list, secon_count))
        self.secon_count_dict = sort_adict(self.secon_count_dict)
        self.secon_cate_list = list(self.secon_count_dict.keys())

    def _goods_tab(self):
        """
        方法用于
        """
        tab_name1 = ["车厘子莓类", "柑橘橙柚", "苹果梨", "葡萄提子", "蜜瓜西瓜",
                     "桃", "绿叶菜", "茄果瓜果", "根茎类", "菌菇类"]
        tab_name2 = ["鸡蛋禽蛋", "冷冻水产", "牛肉", "猪肉", "羊肉",
                     "海鲜半成品", "鸡鸭鸽", "牛排", "调味肉制品", "火锅烧烤"]
        tab_name3 = ["酸奶乳酸菌", "蛋糕点心", "下午茶", "薯片膨化", "巧克力", "坚果炒货",
                     "糕点甜品", "糖果果冻", "咖啡茶饮", "冲调谷物", "果蔬汁", "碳酸功能"]
        tab_name4 = ["衣物清洁", "家居日用", "头发护理", "口腔护理",
                     "婴童食品", "婴童护理", "纸制品", "宠物用品"]
        self.tab1_cate_code = self._cate_name_code(self.goods_cate_data, tab_name1)
        self.tab2_cate_code = self._cate_name_code(self.goods_cate_data, tab_name2)
        self.tab3_cate_code = self._cate_name_code(self.goods_cate_data, tab_name3)
        self.tab4_cate_code = self._cate_name_code(self.goods_cate_data, tab_name4)
        self.tab_spu_code["01"] = self._cate_to_spu(self.filter_spu_data, self.tab1_cate_code)
        self.tab_spu_code["02"] = self._cate_to_spu(self.filter_spu_data, self.tab2_cate_code)
        self.tab_spu_code["03"] = self._cate_to_spu(self.filter_spu_data, self.tab3_cate_code)
        self.tab_spu_code["04"] = self._cate_to_spu(self.filter_spu_data, self.tab4_cate_code)

    @staticmethod
    def _cate_name_code(frame, name_list):
        """
        方法用于查找
        """
        return frame[(frame.cate_name.isin(name_list)) &
                     (frame.cate_level == 2)].cate_code.tolist()

    @staticmethod
    def _cate_to_spu(frame, cate_list):
        """
        方法用于查找
        """
        return frame[frame.second_cate.isin(cate_list)].spu_code.tolist()

    def _first_second_spu(self):
        """
        方法用于
        """
        for first_code in self.first_cate_list:
            second_cate = self.goods_cate_data[
                self.goods_cate_data.cate_parent == first_code
            ]["cate_code"].tolist()
            second_cate = list(set(second_cate).intersection(
                set(self.filter_spu_data.second_cate.tolist())))
            second_cate.sort(key=self.secon_cate_list.index)
            second_cate_dict = {}
            for second_code in second_cate:
                second_spu = self.filter_spu_data[
                    self.filter_spu_data.second_cate == second_code
                ]["spu_code"].tolist()
                second_spu.sort(key=self.all_spu_list.index)
                second_cate_dict[second_code] = second_spu
            self.first_second_spu[first_code] = second_cate_dict

    def _com_all_spu_similar(self, param):
        """
        方法用于
        """
        if param == "full":
            self.spu_similar_matrix = np.zeros((len(self.filter_spu_list),
                                                len(self.owners_spu_codes_list)))
            for i in range(len(self.filter_spu_list)):
                for j in range(len(self.owners_spu_codes_list)):
                    if i == j:
                        self.spu_similar_matrix[i, j] = 1
                    else:
                        self.spu_similar_matrix[i, j] = get_match_score(
                            self.all_spu_name_dict[self.filter_spu_list[i]],
                            self.all_spu_name_dict[self.owners_spu_codes_list[j]])
        else:
            self.spu_similar_matrix = None

    def _total_goods_prof(self, param):
        """
        方法用于
        """
        self._goods_list()
        # self._goods_sort()
        self._goods_name()
        self._goods_cate()
        self._goods_tab()
        # self._first_second_spu()
        # self._com_all_spu_similar(param)
