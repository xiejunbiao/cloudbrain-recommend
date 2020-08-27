import json
import traceback
import tornado.web
import tornado.gen
from tornado.concurrent import run_on_executor
from concurrent.futures.thread import ThreadPoolExecutor

from hot_sale_rec.Owners_RecV2.rec_for_hotsale import rechot_main


class RelaBaikeHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)

    def initialize(self, logger, config_result):  ##删除一个输入参数
        self.__logger = logger
        self.config_result = config_result

    def set_default_headers(self):
        self.set_header('Content-type', 'application/json')

    @tornado.gen.coroutine
    def post(self):

        # #不用try catch可以nohup直接打印错误
        # try:
        # self.set_header('Content-Type','application/json;charset=utf-8')
        args = json.loads(str(self.request.body, encoding="utf-8"))
        try:
            j_data = json.loads(args)
        except:
            j_data = args
        # logging.warning(type(j_data))
        spuDict = {key: value for key, value in j_data.items()}

        page_json = yield self.get_rec_main(spuDict)

        self.write(page_json)

    @run_on_executor
    def get_rec_main(self, spuDict):  # 也必须输入self

        # input_dict={'page':pageNum,'rows':rows,'areaCode':areaCode,'cateCode':cateCode,'sortMethod':sortMethod,'spuCodeList':spuCodeList}
        input_json = json.dumps(spuDict, ensure_ascii=False)  ##False解决中文乱码

        # 如何把异常写进日志？
        try:
            page_json = rechot_main(spuDict, self.config_result)
            # page_json="test:"+input_json
            self.__logger.info("input_dict-" + input_json)  ##用input_dict-作为分隔符
            self.__logger.info("page_json-" + page_json)
            #        log.logger.info(input_json)
            #        log.logger.info(page_json)
        except Exception as e:
            # self.__logger.info("error-\n"+e)##用input_dict-作为分隔符
            self.__logger.info("error:")  ##用input_dict-作为分隔符
            self.__logger.info(e)  ##用input_dict-作为分隔符
            self.__logger.info("traceback My:")  ##用input_dict-作为分隔符
            self.__logger.info(traceback.format_exc())  # 返回异常信息的字符串，可以用来把信息记录到log里;
            page_json = {}

        return page_json


