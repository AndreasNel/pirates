import rpyc
from rpyc.utils.server import ThreadedServer
import subprocess
import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s: (%(name)s) %(levelname)s - %(message)s')
ch.setFormatter(formatter)
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
        # logger.info(output)
        return output

    def exposed_verify(self, clues):
        logger.info('Verifying %s clues for pirate %s', len(clues["data"]), clues["id"])
        clues = dict({key: clues[key] for key in clues})
        output = subprocess.getoutput('python3 rummy.pyc -verify {}'.format(json.dumps(str(clues))))
        output = json.loads(output)
        try:
            logger.info("%s: %s, number of pirates: %s", output["status"], output["message"], len(output["data"]))
            for pirate in output["data"]:
                logger.info("Number of clues for pirate %s: %s", pirate["id"], len(pirate["data"]))
        except:
            logger.info("%s: %s", output["status"], output["message"])
        return output

    def exposed_close_server(self):
        logger.info('Shutting down...')
        server.close()

if __name__ == '__main__':
    logger.info('Now we know where the captain is...')
    server = ThreadedServer(RummyService, auto_register=True)
    server.start()
