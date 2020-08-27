# @author : zhaizhengyuan
# @datetime : 2020/8/7 13:49
# @file : user_prof.py
# @software : PyCharm

"""
文件说明：

"""
# from foryou_offline_com.profile.goods_prof import GoodsProf
# from foryou_offline_com.utils.basic_handles import zoom_gen, grade_data
from foryou_offline_com.utils.basic_handles import sort_adict, get_virtual_area


class UsersProf(object):
    """
    UsersProf类用于
    """

    def __init__(self, owner_list, data_pd):
        """
        方法用于
        """
        # GoodsProf.__init__(self)
        self.logger = data_pd.logger
        self.buy_spu_data = data_pd.buy_spu_data
        self.owner_collect_data = data_pd.owner_collect_data
        self.owner_eval_data = data_pd.owner_eval_data
        self.all_spu_list = data_pd.all_spu_list
        self.total_spu_data = data_pd.total_spu_data
        self.first_cate_list = data_pd.first_cate_list
        self.secon_cate_list = data_pd.secon_cate_list
        self.virtual_area = data_pd.virtual_area
        self.owners_spu_dict = {}
        self.owners_first_dict = {}
        self.owners_secon_dict = {}
        self.owners_consume_dict = {}
        self.owners_has_first_dict = {}
        self.owners_area_dict = {}
        self._total_owners(owner_list)

    def _user_goods(self, owner):
        """
        方法用于
        """
        self._owner_buy_spu = self.buy_spu_data[
            self.buy_spu_data.owner_code == owner]
        self._owner_collect_spu = self.owner_collect_data[
            self.owner_collect_data.owner_code == owner]
        self._owner_eval_spu = self.owner_eval_data[
            self.owner_eval_data.owner_code == owner]
        owner_spu_list = list(set(self._owner_buy_spu["spu_code"].tolist() +
                                  self._owner_collect_spu["spu_code"].tolist() +
                                  self._owner_eval_spu["spu_code"].tolist())
                              & set(self.all_spu_list))
        # self.owner_buy_dict = dict(owner_buy_spu["spu_code"].value_counts())
        # owner_collect_spu = self.owner_collect_data[
        #     self.owner_collect_data.owner_code == owner]
        total_count = []
        owner_has_first = []
        for spu in owner_spu_list:
            buy_count = len(self._owner_buy_spu[
                             self._owner_buy_spu.spu_code == spu])
            collect_count = len(self._owner_collect_spu[
                                 self._owner_collect_spu.spu_code == spu])
            eval_count = len(self._owner_eval_spu[
                              self._owner_eval_spu.spu_code == spu])
            total_count.append(1 * buy_count + 1.5 * collect_count + 0.5 * eval_count)
            owner_has_first.append(
                self.total_spu_data[
                    self.total_spu_data.spu_code == spu].first_cate.values[0])
        owner_spu_dict = dict(zip(owner_spu_list, total_count))
        self.owners_spu_dict[owner] = owner_spu_dict
        self.owners_has_first_dict[owner] = list(set(owner_has_first))

    def _user_cates(self, owner):
        """
        方法用于
        """
        # 用户的一级类别偏好得分
        user_buy_first = [len(self.buy_spu_data[
                          (self.buy_spu_data["owner_code"] == owner) &
                          (self.buy_spu_data["first_cate"] == cate)])
                          for cate in self.first_cate_list]
        user_collect_first = [len(self.owner_collect_data[
                              (self.owner_collect_data["owner_code"] == owner) &
                              (self.owner_collect_data["first_cate"] == cate)])
                              for cate in self.first_cate_list]
        user_first = [user_buy_first[i]+user_collect_first[i]
                      for i in range(len(user_buy_first))]
        try:
            user_first_list = [num / sum(user_first) for num in user_first]
        except ZeroDivisionError:
            user_first_list = user_first
        # user_first_zoom = zoom_gen(user_first_list)
        # user_first_score = [grade_data(user_first_zoom, num) for num in user_first_list]
        user_first_dict = dict(zip(self.first_cate_list, user_first_list))
        user_first_dict = sort_adict(user_first_dict)
        self.owners_first_dict[owner] = user_first_dict
        # 用户的二级类别偏好得分
        user_buy_secon = [len(self.buy_spu_data[
                              (self.buy_spu_data["owner_code"] == owner) &
                              (self.buy_spu_data["second_cate"] == cate)])
                          for cate in self.secon_cate_list]
        user_collect_secon = [len(self.owner_collect_data[
                              (self.owner_collect_data["owner_code"] == owner) &
                              (self.owner_collect_data["second_cate"] == cate)])
                              for cate in self.secon_cate_list]
        user_secon = [user_buy_secon[i] + user_collect_secon[i]
                      for i in range(len(user_buy_secon))]
        try:
            user_secon_list = [num / sum(user_secon) for num in user_secon]
        except ZeroDivisionError:
            user_secon_list = user_secon
        # user_secon_zoom = zoom_gen(user_secon_list)
        # user_secon_score = [grade_data(user_secon_zoom, num) for num in user_secon_list]
        user_secon_dict = dict(zip(self.secon_cate_list, user_secon_list))
        user_secon_dict = sort_adict(user_secon_dict)
        self.owners_secon_dict[owner] = user_secon_dict

    # def _user_consume(self, owner):
    #     """
    #     方法用于
    #     """
    #     consume = self.owner_buy_count_data[
    #         self.owner_buy_count_data["owner_code"] == owner]
    #     consume_ave = consume["sum2"] / consume["sum1"]
    #     consume_data = self.owner_buy_count_data["sum2"] / self.owner_buy_count_data["sum1"]
    #     consume_list = consume_data.tolist()
    #     # 生成用户的消费水平区间
    #     consume_zooms = zoom_gen(consume_list)
    #     # 用户的消费水平偏好得分
    #     user_consume_score = grade_data(consume_zooms, consume_ave.values[0])
    #     self.owners_consume_dict[owner] = user_consume_score

    def _user_areas(self, owner):
        """
        方法用于
        """
        user_area_list = self.buy_spu_data[
            self.buy_spu_data.owner_code == owner].rec_area_code.tolist()
        self.virtual_area_code = get_virtual_area(self.virtual_area)
        self.owners_area_dict[owner] = list(set(user_area_list))

    def _total_owners(self, owner_list):
        """
        方法用于
        """
        for owner in owner_list:
            if owner == "000":
                pass
            else:
                self._user_goods(owner)
                self._user_cates(owner)
                self._user_areas(owner)
