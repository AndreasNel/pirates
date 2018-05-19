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

MAX_QUEUE_LENGTH = 4


class PirateService(rpyc.Service):
    pirate_id = None
    solved = {}
    leader = tuple()
    pending = 0
    tasks = []

    def dig_sand(self, clue):
        logger.info('Digging in the sand')
        result = clue
        result = shovel(result, 100)
        # The result from the bucket function is the same after 5 iterations as after 200 iterations. This is due to the
        # function values repeating for every 3 iterations after the first 5. It is therefore redundant to perform 195
        # operations that continuously yield repeating values.
        for _ in range(5):
            result = bucket(result)
        result = shovel(result, 100)
        return result

    def search_river(self, clue):
        logger.info('Searching the river')
        result = clue
        # The result from the bucket function is the same after 5 iterations as after 200 iterations. This is due to the
        # function values repeating for every 3 iterations after the first 5. It is therefore redundant to perform 195
        # operations that continuously yield repeating values.
        for _ in range(5):
            result = bucket(result)
        return result

    def crawl_into_cave(self, clue):
        logger.info('Crawling into the cave')
        result = clue
        # Using the rope 5 times yields the exact same result as using the rope 200 times. This is due to the function
        # values of the rope function repeating every 5 iterations after the first 5.
        # result = rope(result, 5)
        for _ in range(5):
            result = rope(result)
        for _ in range(100):
            result = torch(result)
        return result

    def exposed_get_pending(self):
        return self.pending

    def wait_pending(self):
        while self.pending:
            time.sleep(0.5)

    def exposed_solve_all(self, clue_list):
        results = [self.exposed_solve(clue) for clue in clue_list]
        return results

    def exposed_solve(self, clue):
        self.wait_pending()
        self.pending += 1
        self.tasks.append(clue)
        try:
            logger.info('Starting the solving process')
            result = clue.key
            result = self.dig_sand(result)
            result = self.search_river(result)
            result = self.crawl_into_cave(result)
            logger.info("Hashing")
            result = hashlib.md5(result.encode('utf-8')).hexdigest()
            result = result.upper()
            logger.info('Solved')
            return {"id": self.exposed_get_id(), "data": result}
        finally:
            self.pending -= 1

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
        clues = get_clues()
        self.distribute_work(clues)

    def exposed_host(self):
        return server.host

    def exposed_port(self):
        return server.port

    def distribute_work(self, clues):
        logger.info('Starting work distribution')
        pirates = discover('Pirate')
        tasks = [[] for _ in range(len(pirates))]
        logger.debug('Empty tasklists: %s', tasks)
        logger.info('Evenly splitting up the clues')
        logger.debug('All clues: %s', clues)
        for index, clue in enumerate(clues["data"]):
            tasks[index % len(tasks)].append(clue)
        logger.debug(tasks)
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
                result = request(tasks[index])
                result.add_callback(self.add_to_queue)
        self.exposed_solve_all(tasks[myindex])

    def add_to_queue(self, result):
        logger.info('Adding clues to queue')
        for clue in result.value:
            self.solved.setdefault(clue.id, []).append(clue.data)
        if max(len(self.solved[clue.id]) for clue in result.value) > MAX_QUEUE_LENGTH:
            self.verify()

    def redistribute(self, results):
        if results.value.get('finished', False):
            logger.info('Finished, closing servers')
            pirates = discover('Pirate')
            for pirate in pirates:
                if (self.exposed_host(), self.exposed_port()) != pirate:
                    c = rpyc.connect(pirate[0], pirate[1])
                    stop = rpyc.async(c.root.close_server)
                    stop()
        else:
            logger.info('Redistributing clues again')
            self.distribute_work(results.value)

    def verify(self):
        data = [{"id": key, "data": self.solved[key]} for key in self.solved]
        request = rpyc.async(rummy.root.verify)
        request(data)
        request.add_callback(self.redistribute)

    def remove_pirate(self, scallywag):
        # TODO handle removal of scallywag
        # TODO determine whether a new pirate should be added
        pass

    def recruit(self, number):
        # TODO recruit the specified number of pirates
        # TODO initiate a new quartermaster election if new pirates are more honest
        pass

    def exposed_close_server(self):
        logger.info('Shutting down...')
        server.close()


if __name__ == '__main__':
    rummy_details = discover('Rummy')
    rummy = rpyc.connect(rummy_details[0][0], rummy_details[0][1])
    server = ThreadedServer(PirateService, auto_register=True, logger=logger)
    server.start()
