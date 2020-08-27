# @author : zhaizhengyuan
# @datetime : 2020/8/10 10:26
# @file : offline_com_rec_result.py
# @software : PyCharm

"""
文件说明：

"""
from foryou_offline_com.profile.user_prof import UsersProf
from foryou_offline_com.utils.basic_handles import sort_adict
from foryou_offline_com.utils.basic_handles import get_match_score
# from foryou_offline_com.utils.basic_handles import resort_by_multiple


class OfflineCom(UsersProf):
    """
    类用于
    """

    def __init__(self, owner_list, data_pd, param):
        """
        方法用于
        """
        UsersProf.__init__(self, owner_list, data_pd)
        self.logger = data_pd.logger
        self.filter_spu_list = data_pd.filter_spu_list
        self.filter_service_list = data_pd.filter_service_list
        self.owners_spu_codes_list = data_pd.owners_spu_codes_list
        self.goods_scope_data = data_pd.goods_scope_data
        self.filter_spu_data = data_pd.filter_spu_data
        self.all_spu_name_dict = data_pd.all_spu_name_dict
        self.tab_spu_code = data_pd.tab_spu_code
        # self.similar_matrix = data_pd.spu_similar_matrix
        self.owners_rec_result = {}
        self.owners_hotsale_rec = {}
        self.owners_foryou_rec = {}
        self.owners_guesslike_rec = {}
        self.owner_areas_spu_dict = {}
        owner_list = list(set(owner_list + ["000"]))
        self._com_rec_result(owner_list, param)

    def _com_owner_rec(self, owner_code, param):
        """
        方法用于
        """
        if owner_code == "000":
            self.owners_rec_result["000"] = self.filter_spu_list
        else:
            owner_spu_dict = self.owners_spu_dict[owner_code]
            owner_first_dict = self.owners_first_dict[owner_code]
            owner_secon_dict = self.owners_secon_dict[owner_code]
            owner_spu_score_dict = {}
            owner_areas_spu_list = self.goods_scope_data[
                self.goods_scope_data["area_code"].isin(
                    self.owners_area_dict[owner_code] + self.virtual_area_code)].spu_code.tolist()
            owner_areas_spu_list = list(set(owner_areas_spu_list) & set(self.filter_spu_list))
            self.owner_areas_spu_dict[owner_code] = owner_areas_spu_list
            for spu_code in self.owner_areas_spu_dict[owner_code]:
                spu_first_score = owner_first_dict[
                    self.filter_spu_data[
                        self.filter_spu_data.spu_code == spu_code].first_cate.values[0]]
                spu_secon_score = owner_secon_dict[
                    self.filter_spu_data[
                        self.filter_spu_data.spu_code == spu_code].second_cate.values[0]]
                spu_code_score = 0.1 * spu_first_score + 0.5 * spu_secon_score
                for owner_spu in list(owner_spu_dict.keys()):
                    # if param == "full" and owner_spu in self.owners_spu_codes_list:
                    #     match_score = self.similar_matrix[
                    #         self.filter_spu_list.index(spu_code)][
                    #         self.owners_spu_codes_list.index(owner_spu)]
                    # else:
                    match_score = get_match_score(self.all_spu_name_dict[owner_spu],
                                                  self.all_spu_name_dict[spu_code])
                    spu_code_score += owner_spu_dict[owner_spu] * match_score
                if spu_code in list(owner_spu_dict.keys()):
                    spu_code_score += 0.5
                owner_spu_score_dict[spu_code] = spu_code_score
            owner_spu_score_dict = sort_adict(owner_spu_score_dict)
            self.owners_rec_result[owner_code] = list(owner_spu_score_dict.keys())

    # def _com_hotsale_rec_result(self, owner_code):
    #     """
    #     方法用于
    #     """
    #     hotsale_first_spu_dict = {}
    #     if owner_code == "000":
    #         for first_cate in self.first_cate_list:
    #             second_cate_dict = {}
    #             second_cate_len = []
    #             for second_cate in self.first_second_spu[first_cate].keys():
    #                 second_cate_len.append(len(
    #                     self.first_second_spu[first_cate][second_cate]))
    #                 second_cate_dict[second_cate] = self.first_second_spu[first_cate][second_cate]
    #             sorted_first_spu_list = resort_by_multiple(
    #                 second_cate_len, second_cate_dict)
    #             hotsale_first_spu_dict[first_cate] = sorted_first_spu_list
    #     else:
    #         for first_cate in list(self.owners_has_first_dict[owner_code]):
    #             owner_second_cate_list = list(self.owners_secon_dict[owner_code].keys())
    #             first_second_cate = list(self.first_second_spu[first_cate].keys())
    #             first_second_cate.sort(key=owner_second_cate_list.index)
    #             second_cate_dict = {}
    #             second_cate_len = []
    #             for second_cate in first_second_cate:
    #                 second_spu_list = list(set(self.first_second_spu[first_cate][second_cate]) &
    #                                        set(self.owner_areas_spu_dict[owner_code]))
    #                 second_spu_list.sort(key=self.owners_rec_result[owner_code].index)
    #                 second_cate_dict[second_cate] = second_spu_list
    #                 second_cate_len.append(len(second_spu_list))
    #             sorted_first_spu_list = resort_by_multiple(
    #                 second_cate_dict, second_cate_len)
    #             hotsale_first_spu_dict[first_cate] = sorted_first_spu_list
    #     return hotsale_first_spu_dict

    def _com_foryou_rec_result(self, owner_code):
        """
        方法用于
        """
        if owner_code == "000":
            self.owners_foryou_rec[owner_code] = self.filter_spu_list
        else:
            for area_code in self.owners_area_dict[owner_code]:
                owner_area_spu_list = self.goods_scope_data[
                    self.goods_scope_data["area_code"].isin(
                        [area_code] + self.virtual_area_code)].spu_code.tolist()
                owner_area_spu_list = list(set(owner_area_spu_list) & set(self.filter_spu_list))
                owner_area_spu_list.sort(key=self.owners_rec_result[owner_code].index)
                self.owners_foryou_rec[owner_code + "_" + area_code] = owner_area_spu_list

    def _com_guesslike_rec_result(self, owner_code):
        """
        方法用于
        """
        if owner_code == "000":
            for tab_code in self.tab_spu_code.keys():
                owner_tab_spu = list(
                    set(self.filter_service_list) &
                    set(self.tab_spu_code[tab_code]))
                owner_tab_spu.sort(key=self.filter_service_list.index)
                self.owners_guesslike_rec[owner_code + "_" +
                                          tab_code] = owner_tab_spu
        else:
            for area_code in self.owners_area_dict[owner_code]:
                owner_area_spu_list = self.goods_scope_data[
                    self.goods_scope_data["area_code"].isin(
                        [area_code] + self.virtual_area_code)].spu_code.tolist()
                owner_area_spu_list = list(set(owner_area_spu_list) & set(self.filter_service_list))
                # owner_area_spu_list = list(set(owner_area_spu_list))
                for tab_code in self.tab_spu_code.keys():
                    owner_area_tab_spu = list(
                        set(owner_area_spu_list) &
                        set(self.tab_spu_code[tab_code]))
                    owner_area_tab_spu.sort(key=self.owners_rec_result[owner_code].index)
                    self.owners_guesslike_rec[owner_code + "_" +
                                              area_code + "_" + tab_code] = owner_area_tab_spu

    def _com_rec_result(self, owner_list, param):
        """
        方法用于
        """
        # self.owners_rec_result["000000"] = self.sorted_filter_spu_list

        for owner_code in owner_list:
            self.logger.info(owner_code)
            self._com_owner_rec(owner_code, param)
            # self.owners_hotsale_rec[owner_code] = self._com_hotsale_rec_result(owner_code)
            self._com_foryou_rec_result(owner_code)
            self._com_guesslike_rec_result(owner_code)
