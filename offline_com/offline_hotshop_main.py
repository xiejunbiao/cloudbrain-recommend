import logging
from hotshop_offline_com.utils.config import ModifyConfFile, log_logger
from hotshop_offline_com.ini_and_update import start

logging.basicConfig()


def get_logger(logger_name=None):
    logger = logging.getLogger(logger_name)
    logger.propagate = False
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger


def start_main():
    # logger = log_logger()
    logger = get_logger("RECHOTSHOP")
    mcf = ModifyConfFile(logger)
    two_mysql = mcf.return_argv()
    start(two_mysql, logger)


if __name__ == '__main__':
    start_main()
