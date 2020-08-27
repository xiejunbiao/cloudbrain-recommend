import sys

sys.path.append("../") ## 会从上一层目录找到一个package

from hot_sale_rec.Owners_RecV2.hotsale_views import HotSale
import time
# from searchMatchV2.utils.dataBaseHandle import readGood_areas#把全局变量，运行一次，数据库配置。放到内存里面


def rechot_main(spuDict,config_result):
    
    hotsale= HotSale(spuDict, config_result)

    return hotsale.handle_request()







