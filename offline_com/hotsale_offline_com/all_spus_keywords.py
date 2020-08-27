import jieba.analyse
from hotsale_offline_com.utils.basic_handles import sort_by_count,\
    buy_times
from hotsale_offline_com.utils.dataBaseHandle import readMysqlmultiPd

###获取所有商品的名称、类别信息
def read_all_goods_info(database):

    # mysqlCmd="SELECT a.spu_code,a.spu_name,\
    # b.cate_parent as second_cate,a.cate_code as third_cate FROM \
    # goods_spu a,\
    # goods_cate b where \
    # a.cate_code = b.cate_code"
    mysqlCmd = "SELECT spu_code,spu_name,first_cate,second_cate,third_cate FROM cb_goods_spu_for_filter where goods_status = 1 and store_status =1 and first_cate!=''and second_cate!=''"

    dataPd = readMysqlmultiPd(mysqlCmd,database)
    #mysqlCmd = "SELECT cate_code,cate_parent from goods_cate"
    mysqlCmd = "SELECT spu_code,spu_name,first_cate,second_cate,third_cate from cb_owner_buy_goods_info where first_cate is not null and second_cate is not null"
    dataPd1 = readMysqlmultiPd(mysqlCmd,database)
    # cate_code = dataPd1["cate_code"].tolist()
    # cate_parent = dataPd1["cate_parent"].tolist()
    # cate_second = dataPd["second_cate"].tolist()
    # cate_first = []
    # for i in range(len(dataPd)):
    #    first=cate_parent[cate_code.index(cate_second[i])]
    #    try:
    #       first=int(first)
    #    except ValueError:
    #       first=int(cate_second[i])
    #    cate_first.append(first)
    # dataPd['first_cate'] = cate_first

    dataPd_all = dataPd[['spu_code','spu_name','first_cate','second_cate','third_cate']].append(dataPd1[['spu_code','spu_name','first_cate','second_cate','third_cate']])
    dataPd_all = dataPd_all.drop_duplicates()
    dataPd_all.index = list(range(dataPd_all.shape[0]))
    return dataPd_all

##通过类的方法获取所有商品的销量、一级类别、关键词字典
def get_all_goods_keywords_dict(dataDf,dataDf1):

    spuname_dataDf = dataDf['spu_name'].tolist()
    spucode_dataDf = dataDf['spu_code'].tolist()
    first_dataDf = dataDf['first_cate'].tolist()
    second_dataDf = dataDf['second_cate'].tolist()
    cate_order_total = buy_times(dataDf1, 'first_cate','')
    ##所有商品一级类别代码列表
    cate_code_list = list(cate_order_total.keys())
    ##统计下单商品
    spu_code_total = buy_times(dataDf1, 'spu_code','')

    for spu_code in spucode_dataDf:

        if spu_code in list(spu_code_total.keys()):
            temp = spu_code_total[spu_code]+1
            spu_code_total[spu_code] = temp
        else:
            spu_code_total[spu_code] = 1
    ##所有商品代码列表
    spu_code_list = list(spu_code_total.keys())
    ragd = rec_all_goods_dict(spucode_dataDf, first_dataDf,second_dataDf,spuname_dataDf,\
                              spu_code_total, cate_order_total,spu_code_list,cate_code_list)
    goods_keywords_dict = ragd.goods_keywords_dict
    cate_goods_dict = ragd.cate_goods_dict
    return goods_keywords_dict,cate_goods_dict

##定义一个推荐商品字典，计算商品的销量，一级类别，关键词列表，主键是商品名称
class rec_all_goods_dict():

    def __init__(self,spucode_dataDf, first_dataDf,second_dataDf, spuname_dataDf,spu_code_total,\
                               cate_order_total,spu_code_list,cate_code_list):

        self.goods_keywords_dict = self.all_goods_keywords(spucode_dataDf, first_dataDf,\
                               spuname_dataDf,spu_code_total, cate_order_total,spu_code_list)
        self.cate_goods_dict = self.all_cate_goods(first_dataDf,second_dataDf,\
                                                   spucode_dataDf,cate_code_list)

    def all_goods_keywords(self,spucode_dataDf, cate_dataDf, spuname_dataDf, \
                               spu_code_total, cate_order_total,spu_code_list):
        ##字典形式存储每个商品名称对应的商品销量字典，一级类别销量字典以及商品关键词列表
        ##其中主键是商品名称，
        ##第一个值是商品销量字典，字典的键是商品代码，值是销量
        ##第二个值是一级类别销量字典，字典的键是一级类别代码，值是销量
        ##第三个值是商品关键词列表
        goods_keywords_dict = {}
        for spu_code in spu_code_list:
            result = self.name_spu_cate_keywords(spucode_dataDf, cate_dataDf, spuname_dataDf, spu_code_total,
                                            cate_order_total,spu_code)
            goods_keywords_dict[spu_code]=result
        return goods_keywords_dict

    ##计算商品的购买次数，商品所在一级类别的购买次数以及商品名称的jieba分词列表
    ##输入参数分别为商品代码列表数据，一级类别代码列表数据，商品名称列表数据
    ##所有商品名称的购买字典，所有一级类别的购买字典以及商品名称
    def name_spu_cate_keywords(self,spucode_dataDf, cate_dataDf, spuname_dataDf, \
                               spu_code_total, cate_order_total, spu_code):
        spu_name = spuname_dataDf[spucode_dataDf.index(spu_code)]  # 商品代码
        cate_code = cate_dataDf[spucode_dataDf.index(spu_code)]  # 一级类别代码
        #print(cate_code)
        spu_dict = {}
        # 该商品的购买次数字典，主键是该商品名称，值是商品购买次数
        spu_dict[spu_name] = spu_code_total[spu_code]
        #cate_code_dict = {}
        # 该商品的一级类别购买次数字典，主键是该一级类别代码，值是购买次数
        try:
            spu_dict[cate_code] = cate_order_total[cate_code]
        except KeyError:
            spu_dict[cate_code] = 100
            # jieba分词提取的关键词列表
        spu_name_keywords = self.jieba_keywords(spu_name)
        spu_name_key = spu_name+"key_words"
        spu_dict[spu_name_key] = spu_name_keywords
        return spu_dict

    ##jieba分词提取spu_name中的关键词，返回关键词列表
    def jieba_keywords(self,spu_name):
        cixing = ('ns', 'n', 'nr', 'vn', 'v', 'i', 'a', 'e','y',\
                  'ad', 'an', 'j', 'nz', 'Ng', 'vd', 'un', 'dg','o',\
                  'ng', 'nt', 'nl', 'nr2', 's', 'f', 'vx', 'vi', 'vl', 'Ag',\
                  'vg', 'ag', 'al', 'bl', 'z', 'rz', 'r', 'b', 'l', 'g', 'nz')
        list = jieba.analyse.extract_tags(spu_name, topK=20, withWeight=False, allowPOS=cixing)
        return list

    def all_cate_goods(self,first_data,second_data,spu_code_data,all_first_list):

        ##字典形式存储每个一级类别代码所对应的二级类别和商品名称，
        ##其中键是一级类别代码，值是二级类别字典
        ##二级类别字典中，键是二级类别代码，值是该二级类别对应的商品名称
        first_spus_dict = {}
        for first_cate in all_first_list:

            sorted_all_second_list = self.get_datalist_from_index(\
                first_cate,first_data,second_data)
            second_spus_dict = {}
            for second_cate in sorted_all_second_list:
                sorted_spu_code_list = self.get_datalist_from_index(\
                second_cate,second_data,spu_code_data)
                second_spus_dict[second_cate] = sorted_spu_code_list
            first_spus_dict[first_cate] = second_spus_dict
        return first_spus_dict

    def get_datalist_from_index(self, temp, first_data, second_data):

        first_list = [i for i, x in enumerate(first_data) \
                      if x == temp]
        second_list = [x[1] for x in enumerate(second_data) \
                       if x[0] in first_list]
        all_second_list = list(set(second_list))
        sorted_all_second_list = sort_by_count(second_data,all_second_list)
        return sorted_all_second_list