from tools import shovel, rope, torch, bucket
import hashlib
import rpyc
from rpyc.utils.server import ThreadedServer
from rpyc.utils.factory import discover
import logging
import time

logger = logging.getLogger()
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


class PirateService(rpyc.Service):
    pirate_id = None
    solved = []
    leader = tuple()

    def dig_sand(self, clue_string):
        logger.debug('Digging in the sand')
        result = clue_string
        result = shovel(result, 100)
        # The result from the bucket function is the same after 5 iterations as after 200 iterations. This is due to the
        # function values repeating for every 3 iterations after the first 5. It is therefore redundant to perform 195
        # operations that continuously yield repeating values.
        for _ in range(5):
            result = bucket(result)
        result = shovel(result, 100)
        return result

    def search_river(self, clue_string):
        logger.debug('Searching the river')
        result = clue_string
        # The result from the bucket function is the same after 5 iterations as after 200 iterations. This is due to the
        # function values repeating for every 3 iterations after the first 5. It is therefore redundant to perform 195
        # operations that continuously yield repeating values.
        for _ in range(5):
            result = bucket(result)
        return result

    def crawl_into_cave(self, clue_string):
        logger.debug('Crawling into the cave')
        result = clue_string
        # Using the rope 5 times yields the exact same result as using the rope 200 times. This is due to the function
        # values of the rope function repeating every 5 iterations after the first 5.
        # result = rope(result, 5)
        for _ in range(5):
            result = rope(result)
        for _ in range(100):
            result = torch(result)
        return result

    def exposed_solve_all(self, data):
        logger.info('look here')
        # logger.info(data)
        logger.info(data)
        logger.info('stop here')
        return self.solve_all(data)

    def solve_all(self, data):
        pirate_id = data["id"]
        clue_list = data["data"]
        results = list([self.exposed_solve(clue) for clue in clue_list])
        return {"id": pirate_id, "data": results}

    def exposed_solve(self, clue):
        # logger.debug(clue)
        clue_id = clue["id"]
        clue_data = clue["data"]
        # logger.debug(clue_data)
        # logger.debug(clue_id)
        logger.info('Starting the solving process for clue %s', clue_id)
        result = clue_data
        result = self.dig_sand(result)
        result = self.search_river(result)
        result = self.crawl_into_cave(result)
        logger.debug("Hashing")
        result = hashlib.md5(result.encode('utf-8')).hexdigest()
        result = result.upper()
        logger.info('Solved')
        return {"id": clue_id, "key": result}

    def exposed_set_id(self, pirate_id):
        logger.debug('Setting ID: {}'.format(pirate_id))
        self.pirate_id = pirate_id

    def exposed_get_id(self):
        return self.pirate_id

    def exposed_is_quartermaster(self):
        return (self.exposed_host(), self.exposed_port()) == self.leader

    def exposed_elect_leader(self):
        logger.info('Electing a new leader')
        pirates = discover('Pirate')
        logger.debug('Pirates available: %s', pirates)
        pirates = sorted(pirates)
        logger.debug('Sorted pirates: %s', pirates)
        self.leader = (pirates[0][0], pirates[0][1])
        logger.info('Found a new leader: %s', self.leader)
        for pirate in pirates:
            if (self.exposed_host(), self.exposed_port()) != pirate:
                c = rpyc.connect(pirate[0], pirate[1])
                set_leader = c.root.set_leader
                set_leader(self.leader)
        return self.leader

    def exposed_set_leader(self, l):
        self.leader = l

    def exposed_start(self):
        logger.info('This is the leader, starting the process')
        get_clues = rummy.root.clues
        result = get_clues()
        self.distribute_work(result["data"])

    def exposed_host(self):
        return server.host

    def exposed_port(self):
        return server.port

    def distribute_work(self, clues):
        logger.info('Starting work distribution')
        logger.debug(len(clues))
        for clue in clues:
            logger.debug('pirate["id"]: %s', clue["id"])
            logger.debug('num clues: %s', len(clue['data']))
        pirates = discover('Pirate')
        # tasks = []
        # logger.debug('Empty tasklists: %s', tasks)
        logger.info('Evenly splitting up the clues')
        # logger.debug('All clues: %s', clues)
        # for index, clue in enumerate(clues):
            # logger.info('Appending clue: %s (%s clues)', clue["id"], len(clue["data"]))
            # tasks.append(clue)
        # logger.debug(len(tasks))
        myindex = 0
        for index, pirate in enumerate(pirates):
            if pirate == (self.exposed_host(), self.exposed_port()):
                myindex = index
                logger.debug('My index: %s', myindex)
                continue
            else:
                logger.info('Sending clues to pirate at %s', pirates[index])
                c = rpyc.connect(pirate[0], pirate[1])
                request = rpyc.async(c.root.solve_all)
                logger.debug('Number of clues to send: %s', len(clues[index]))
                result = request(clues[index])
                result.add_callback(self.exposed_verify)
        logger.debug('Number of clues for me: %s', len(clues[myindex]))
        myresults = self.solve_all(clues[myindex])
        self.verify(myresults)

    def redistribute(self, results):
        logger.debug('Redistribute method called')
        if results.value.get('finished', False):
            logger.info('Finished, closing servers')
            pirates = discover('Pirate')
            for pirate in pirates:
                if (self.exposed_host(), self.exposed_port()) != pirate:
                    c = rpyc.connect(pirate[0], pirate[1])
                    stop = rpyc.async(c.root.close_server)
                    stop()
            self.exposed_close_server()
        else:
            logger.info('Redistributing clues again')
            new_clues = results.value["data"]
            logger.debug('Redistibuting: %s', new_clues)
            self.distribute_work(new_clues)

    def exposed_verify(self, results):
        logger.debug('Verify callback called...')
        self.verify(results.value)

    def verify(self, results):
        logger.info('Verifying with the captain...')
        request = rpyc.async(rummy.root.verify)
        request(results)
        request.add_callback(self.redistribute)

    def exposed_close_server(self):
        logger.info('Shutting down...')
        server.close()


if __name__ == '__main__':
    rummy_details = discover('Rummy')
    rummy = rpyc.connect(rummy_details[0][0], rummy_details[0][1])
    server = ThreadedServer(PirateService, auto_register=True, logger=logger)
    server.start()
