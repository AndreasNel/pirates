from rpyc.utils.registry import UDPRegistryServer
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s: (PirateWatch) %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

class PirateWatch(UDPRegistryServer):
    def on_service_removed(self, name, addrinfo):
        if not self.services.items():
            logger.info('All services have deregistered')
            self.close()

if __name__ == '__main__':
    logger.info('Starting up UDP registry server...')
    PirateWatch(logger=logger).start()
