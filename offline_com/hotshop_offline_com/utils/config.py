import sys
import os
import getopt
import configparser
import logging
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(os.path.split(rootPath)[0])
sys.path.append(rootPath)


def para_set(argv):
    ip = ''
    port = ''
    conf = ''
    try:
        opts, args = getopt.getopt(argv, "hc:l:d", ["conf=", "log=", "db="])
    except getopt.GetoptError:
        print('test.py -i <inputfile> -p <outputfile>')
        sys.exit(2)
    for opt, arg in opts:
        print(opt, arg)
        if opt == '-h':
            print('test.py -i <inputfile> -p <outputfile>')
            sys.exit()
        elif opt in ("-c", "--conf"):
            conf = arg
        elif opt in ("-l", "--log"):
            log = arg
        elif opt in ("-d", "--db"):
            db = arg

    #   self.__logger.info('输入的文件为：', ip)
    #   self.__logger.info('输出的文件为：', port)
    #   self.__logger.info('db:',db)
    # return (conf,log)
    return conf


class ConfigValue:
    """
    配置文件类
    """
    # 项目路径
    # rootDir = os.path.split(os.path.realpath(__file__))[0]
    # config.ini文件路径
    # configFilePath = os.path.join(rootDir, 'config.ini')
    def __init__(self, rootdir):
        configfilepath = os.path.join(rootdir, 'config.ini')
        self.config = configparser.ConfigParser()
        self.config.read(configfilepath)

    def get_config_values(self, section_argv, arglist=None):
        """
        根据传入的section获取对应的value
        :param section_argv: ini配置文件中用[]标识的内容
        :return:
        """
        # return config.items(section=section)
        if arglist is None:
            arglist = ['ip', 'user', 'password', 'db', 'port']

        # if section_argv == 'dbtable':
        #     arglist = ['order_table', 'rec_table', 'filter_table']
        config_result = {}
        for option in arglist:
            if option == 'envIp':
                section = 'storeinterface'
            else:
                section = '%s' % section_argv
            config_result[option] = self.config.get(section=section, option=option)
        return config_result

    def get_config_values_list(self, section_argv, arglist) -> list:
        """
        获得某个配置文件中指定section(section_argv),和指定option列表(arglist)对应的值
        """
        config_result = []
        for option in arglist:
            if option == 'envIp':
                section = 'storeinterface'
            else:
                section = '%s' % section_argv
            config_result = config_result + str(self.config.get(section=section, option=option)).split(',')

        return config_result

    def get_sections_all(self) -> list:
        """
        获得某个配置文件的所有section
        """
        return self.config.sections()

    def get_option(self, section):
        """
        获得某个section的所有option
        """
        return self.config.options(section)

    def get_key_values(self, section):
        return self.config.items(section)


cmd_argv = sys.argv[1:]
# cmd_argv = "--conf D:\hisense"

class ModifyConfFile(object):

    def __init__(self, logger):
        self.logger = logger
        try:
            self._file_path_argv = para_set(cmd_argv)  # 'D:\hisense'

            self._config_Value = ConfigValue(self._file_path_argv)

            self._sqyn_mysql = self._config_Value.get_config_values('mysql_2')

            self._hisense_mysql = self._config_Value.get_config_values('mysql_1')
        except Exception as e:
            self.logger.debug(e)

    def return_argv(self):
        return {'hisense_mysql': self._hisense_mysql, 'sqyn_mysql': self._sqyn_mysql}


def log_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)
    handler = logging.FileHandler("log.log")
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    # logger.info("Start print log")
    # logger.debug("Do something")
    # logger.warning("Something maybe fail.")
    # logger.info("Finish")
    return logger


# E:\Document\project\cloudbrain-importer-sensorsdata\importer-sensorsdata\format_importer_1_13_2\conf


if __name__ == '__main__':
    pass
    # logger = log_logger()
    # mcf = ModifyConfFile(logger)
    # dict = mcf.return_argv()
    # for key in dict.keys():
    #      print(dict[key])