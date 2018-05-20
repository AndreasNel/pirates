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

class PirateWatch(UDPRegistryServer):
    def on_service_removed(self, name, addrinfo):
        if not self.services.items():
            logger.info('All services have deregistered')
            self.close()

if __name__ == '__main__':
    logger.info('Starting up UDP registry server...')
    PirateWatch(logger=logger).start()
