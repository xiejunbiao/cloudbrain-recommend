import sys
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.httpclient
import tornado.web
import tornado.gen
import logging
# # 服务器上改成绝对路径
sys.path.append("/opt/ado-services/cloudbrain-recommend/cloudbrain-recommend/online_server/handlers")
sys.path.append("/opt/ado-services/cloudbrain-recommend/cloudbrain-recommend/offline_com")
from order_spu_rec.order_spu_rec_handler import OrderSpuRecHandler
from hot_shop_rec.hot_shop_handler import HotShopHandler
from always_use.always_use_handler import AlwaysUseHandler
from guess_you_like.guess_you_like_handler import GuessYouLikeHandler
from hot_sale_rec.rec_hot_handler import RelaBaikeHandler
from for_you_rec.foryou_handler import ForYouHandler
from online_server.handlers.new_goods_and_dis_hot_sale.data_new_goods_dis_hot_sale_handler import \
    DisHotSaleAndNewGoodsAllHandler, DisHotSaleAndNewGoodsHomeHandler
from online_server.handlers.new_goods_and_dis_hot_sale.new_goods_update_handler import NewGoodsUpdateHandler
from online_server.handlers.new_goods_and_dis_hot_sale.dis_hot_sale_update_handler import DisHotSaleUpdateHandler
from utils.load_data_task import load_area_spu_to_redis
from utils.configInit import config_dict
from utils.rabbitmq_consumer import RabbitConsumer
# from offline_com.handlers.offline_com_handler import OfflineComHandler

logging.basicConfig()


def getLogger(logger_name=None):
    # strPrefix = "%s%d%s" % (strPrefixBase, os.getpid(),".log")##进程的pid命名日志
    logger = logging.getLogger(logger_name)
    logger.propagate = False
    # handler = TimedRotatingFileHandler(strPrefix, 'H', 1)
    # #将http访问记录，程序自定义日志输出到文件，按天分割，保留最近30天的日志。
    # 使用TimedRotatingFileHandler处理器
    #    handler = TimedRotatingFileHandler(strPrefix, when="d", interval=1, backupCount=60)##d表示按天
    #    interval 是间隔时间单位的个数，指等待多少个 when 的时间后 Logger 会自动重建新闻继续进行日志记录
    #    handler = TimedRotatingFileHandler(strPrefix, when="midnight", interval=0, backupCount=60)##
    #    atTime=Time_log_info(23,59,59)
    #    handler = TimedRotatingFileHandler(strPrefix, when="midnight", interval=0, backupCount=60, atTime=atTime)##
    # 实例化TimedRotatingFileHandler
    # interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
    # S 秒、 M 分、 H 小时、 D 天、W 每星期（interval==0时代表星期一）、 midnight 每天凌晨
    ##
    # handler=SafeFileHandler(filename=strPrefix,mode='a')
    #    handler.suffix = "%Y%m%d_%H%M%S.log"
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.INFO)
    # handler.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger


def start():
    consumer = RabbitConsumer(config_dict, getLogger('RABBIT'))
    load_area_spu_to_redis(getLogger('MAIN'))
    port = 6602  # by me
    app = tornado.web.Application(
        handlers=[(r"/cloudbrain-recommend/recommends/gethotsalegoods", RelaBaikeHandler,
                   dict(logger=getLogger("REC_HOT_GOODS"), config_result=config_dict)),
                  (r"/cloudbrain-recommend/recommends/getforyougoods", ForYouHandler,
                   dict(logger=getLogger('REC_FOR_YOU'), config_result=config_dict)),
                  (r"/cloudbrain-recommend/recommends/getownerbuygoods", OrderSpuRecHandler,
                   dict(logger=getLogger('REC_ORDER_SPU'), config_result=config_dict)),
                  (r"/cloudbrain-recommend/recommends/gethotshops", HotShopHandler,
                   dict(logger=getLogger('REC_HOT_SHOP'), config_result=config_dict)),
                  (r"/cloudbrain-recommend/recommends/alwaysuse", AlwaysUseHandler,
                   dict(logger=getLogger('ALWAYS_USE'))),
                  (r"/cloudbrain-recommend/recommends/guessyoulike", GuessYouLikeHandler,
                   dict(logger=getLogger('GUESS_YOU_LIKE'))),
                  (r"/cloudbrain-recommend/recommends/getdishotsalenewgoodsall", DisHotSaleAndNewGoodsAllHandler,
                   dict(logger=getLogger('REC_NEW_GOODS_DISCOUNT_HOT_SALE_ALL'), config_result=config_dict)),
                  (r"/cloudbrain-recommend/recommends/newgoodsupdate", NewGoodsUpdateHandler,
                   dict(logger=getLogger('NEW_GOODS_UPDATE'), config_result=config_dict)),
                  (r"/cloudbrain-recommend/recommends/getdishotsalenewgoodshome", DisHotSaleAndNewGoodsHomeHandler,
                   dict(logger=getLogger('DISCOUNT_HOT_SALE_NEW_GOODS_HOME'), config_result=config_dict)),
                  (r"/cloudbrain-recommend/recommends/dishotsaleupdate", DisHotSaleUpdateHandler,
                   dict(logger=getLogger('DISCOUNT_HOT_SALE_UPDATE'), config_result=config_dict)),
                  # (r"/cloudbrain-recommend/recommends/offline_com_foryou", OfflineComHandler,
                  #  dict(logger=getLogger('OFFLINE_COM_FORYOU'), config_result=config_dict)),
                  ])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.bind(port)
    http_server.start(5)
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()


if __name__ == "__main__":
    log_1 = getLogger('service is start')
    start()
