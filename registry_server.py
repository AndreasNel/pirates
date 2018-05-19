from rpyc.utils.registry import UDPRegistryServer
import logging

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s: (%(name)s) %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)


if __name__ == '__main__':
    logger.info('Starting up UDP registry server...')
    UDPRegistryServer(logger=logger).start()
