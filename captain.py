import rpyc
from rpyc.utils.server import ThreadedServer
import subprocess
import json
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



class RummyService(rpyc.Service):
    def exposed_wake(self):
        logger.info('Waking the captain up...')
        output = subprocess.getoutput('python3 rummy.pyc -wake')
        logger.info(output)
        output = json.loads(output)
        return output

    def exposed_gather(self, number=None):
        logger.info('Gathering {} clues...'.format(number))
        output = subprocess.getoutput('python3 rummy.pyc -gather {}'.format(number if number else ''))
        logger.info(output)
        output = json.loads(output)
        return output

    def exposed_unlock(self):
        logger.info('Unlocking any locks')
        output = subprocess.getoutput('python3 rummy.pyc -unlock')
        logger.info(output)
        output = json.loads(output)
        return output

    def exposed_prepare(self):
        logger.info('Preparing for the voyage')
        output = subprocess.getoutput('python3 rummy.pyc -prepare')
        output = json.loads(output)
        logger.info(output)
        return output

    def exposed_add(self, num_pirates=None):
        logger.info('Adding {} pirates...'.format(num_pirates))
        output = subprocess.getoutput('python3 rummy.pyc -add {}'.format(num_pirates if num_pirates else ''))
        output = json.loads(output)
        logger.info(output)
        return output

    def exposed_remove(self, *pirate_ids):
        logger.info('Removing pirates: {}'.format(str(pirate_ids)))
        output = subprocess.getoutput('python3 rummy.pyc -remove {}'.format(json.dumps(pirate_ids)))
        output = json.loads(output)
        logger.info(output)
        return output

    def exposed_ship_out(self):
        logger.info('Shipping out...')
        output = subprocess.getoutput('python3 rummy.pyc -shipout')
        output = json.loads(output)
        logger.info(output)
        return output

    def exposed_clues(self, pirate_id=None):
        logger.info('Getting clues for {}...'.format(pirate_id))
        output = subprocess.getoutput('python3 rummy.pyc -clues {}'.format(pirate_id if pirate_id else ''))
        output = json.loads(output)
        logger.info(output)
        return output

    def exposed_verify(self, clues):
        logger.info('Verifying clues: {}'.format(str(clues)))
        output = subprocess.getoutput('python3 rummy.pyc -verify {}'.format(json.dumps(clues)))
        output = json.loads(output)
        logger.info(output)
        return output


if __name__ == '__main__':
    logger.info('Now we know where the captain is...')
    ThreadedServer(RummyService, auto_register=True).start()
