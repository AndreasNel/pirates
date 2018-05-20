from tools import shovel, rope, torch, bucket
import socket
import hashlib
import rpyc
from rpyc.utils.server import ThreadedServer
from rpyc.utils.factory import discover
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s: (Pirate) %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class PirateService(rpyc.Service):
    def exposed_set_id(self, new_pirate_id):
        global pirate_id
        logger.debug('Setting ID: {}'.format(new_pirate_id))
        pirate_id = new_pirate_id

    def exposed_get_id(self):
        global pirate_id
        return pirate_id

    def exposed_is_leader(self):
        return (self.exposed_host(), self.exposed_port()) == leader

    def exposed_host(self):
        return myhost

    def exposed_port(self):
        return server.port

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

    def exposed_elect_leader(self):
        global leader
        logger.info('Electing a new leader')
        pirates = discover('Pirate')
        logger.info('Pirates available: %s', pirates)
        pirates = sorted(pirates)
        logger.debug('Sorted pirates: %s', pirates)
        leader = (pirates[0][0], pirates[0][1])
        logger.info('Found a new leader: %s', leader)
        for pirate in pirates:
            if (self.exposed_host(), self.exposed_port()) != pirate:
                c = rpyc.connect(pirate[0], pirate[1])
                set_leader = c.root.set_leader
                set_leader(leader)
        return leader

    def exposed_set_leader(self, l):
        global leader
        logger.info('My leader is being changed to %s', l)
        leader = l
        logger.debug('My leader is: %s', leader)

    def exposed_start(self):
        logger.info('This is the leader, starting the process')
        get_clues = qm.root.clues
        result = get_clues()
        logger.info('%s: %s', result["status"], result["message"])
        self.distribute_work(result["data"])

    def distribute_work(self, pirate_data):
        """
        This method expects pirate_data to be of the form [{"id": pirate_id, "data": [{"id": clue_id, "data": clue_data}]}].
        """
        logger.info('Starting work distribution')
        logger.info("Number of pirates: %s", len(pirate_data))
        all_clues = []
        for pirate in pirate_data:
            logger.debug("Adding %s to global clue list", len(pirate["data"]))
            all_clues.extend(pirate["data"])
        logger.info("Number of clues to be distributed: %s", len(all_clues))
        pirates = discover("pirate")
        logger.info("Found pirates: %s", pirates)
        logger.info("Splitting the clues into %s chunks", len(pirates) - 1)
        chunks = list([all_clues[i::len(pirates)-1] for i in range(len(pirates) - 1)])
        logger.info("Split into %s chunks", len(chunks))
        for chunk in chunks:
            logger.debug("Chunk length: %s", len(chunk))
        logger.debug('All pirates: %s', pirates)
        logger.debug('Leader: %s', leader)
        logger.debug('My host and port: %s', (self.exposed_host(), self.exposed_port()))
        for pirate in pirates:
            if pirate != (self.exposed_host(), self.exposed_port()):
                logger.info("Sending clues to pirate at %s", pirate)
                c = rpyc.connect(pirate[0], pirate[1])
                give_clues = rpyc.async(c.root.set_clues)
                clues = chunks.pop()
                result = give_clues(clues)
                result.wait()

    def exposed_set_clues(self, data):
        """
        This method expects clues to be of the form [{"id": clue_id, "data": clue_data}]
        """
        logger.info("I've got clues apparently")
        clues = data
        if len(clues):
            summary = { "id": self.exposed_get_id(), "data": clues }
            logger.debug("%s: %s clues", summary["id"], len(summary["data"]))
            self.solve_all(summary)

    def solve_all(self, data):
        """
        This method expects data to be an object of the form {"id": pirate_id, "data": [{"id": clue_id, "data": clue_data}]}
        """
        logger.info("Solving all the clues")
        pirate_id = data["id"]
        logger.debug("pirate_id: %s", pirate_id)
        clue_list = data["data"]
        logger.debug("num clues: %s", len(clue_list))
        results = list(self.solve(clue) for clue in clue_list)

        summary = {"id": pirate_id, "data": results}
        pirates = discover('Pirate')
        first_mate = None
        for pirate in pirates:
            if pirate != (self.exposed_host(), self.exposed_port()):
                c = rpyc.connect(pirate[0], pirate[1])
                is_first_mate = c.root.is_leader
                answer = is_first_mate()
                if answer:
                    first_mate = pirate
                    break
        while not first_mate:
            logger.warn("No leader found, time to get a new one (%s)", self.exposed_get_id())
            self.exposed_elect_leader()
            first_mate = leader
        c = rpyc.connect(first_mate[0], first_mate[1])
        verify_clues = rpyc.async(c.root.verify)
        result = verify_clues(summary)
        result.wait()

    def solve(self, clue):
        """
        This method expects clue to be an object of the form {"id": clue_id, "data": clue_data}
        """
        clue_id = clue["id"]
        clue_data = clue["data"]
        logger.info('Solving clue ID %s, length of clue: %s', clue_id, len(clue_data))
        result = clue_data
        result = self.dig_sand(result)
        result = self.search_river(result)
        result = self.crawl_into_cave(result)
        result = hashlib.md5(result.encode('utf-8')).hexdigest().upper()
        return {"id": clue_id, "key": result}

    def exposed_verify(self, data):
        """
        This method expects data to be of the form {"id": pirate_id, "data": [{"id": clue_id, "data": clue_data}]}
        """
        logger.info('Verifying with the captain...')
        request = qm.root.verify
        results = request(data)
        logger.info("%s: %s", results["status"], results["message"])
        try:
            finished = results["finished"]
        except Exception:
            finished = False
        if finished:
            logger.info("Finally I'm done. Now to do Captain Rummy's dirty work so that we can split the treasure. Time to kill the crew.")
            pirates = discover('Pirate')
            for pirate in pirates:
                if pirate != (self.exposed_host(), self.exposed_port()):
                    c = rpyc.connect(pirate[0], pirate[1])
                    kill = rpyc.async(c.root.close_server)
                    kill()
            logger.info('And now that bastard of a quartermaster.')
            kill = rpyc.async(qm.root.close_server)
            kill()
            logger.info("Wait, Captain Rummy, don't shoot! What about our pla--?")
            self.exposed_close_server()
        else:
            try:
                data = results["data"]
            except:
                logger.info("I'm done, I wonder what the others are up to...")
                data = []
            if data:
                logger.info("Not finished, redistributing failed clues")
                self.distribute_work(results["data"])

    def exposed_close_server(self):
        logger.info('Shutting down...')
        server.close()

if __name__ == '__main__':
    # Get the local IP address of this pirate. The ThreadedServer#server attribute is not used, due to it always returning '0.0.0.0'.
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    myhost = s.getsockname()[0]
    s.close()
    leader = tuple()
    pirate_id = None
    qm_details = discover('QuarterMaster')
    if len(qm_details) != 1:
        raise Exception('One and only one quartermaster should exist before trying to wake a pirate up')
    qm = rpyc.connect(qm_details[0][0], qm_details[0][1])
    server = ThreadedServer(PirateService, auto_register=True, logger=logger)
    server.start()
