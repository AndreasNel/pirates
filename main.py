import psutil
import rpyc
from rpyc.utils.factory import discover
import logging
import subprocess

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

DEV_MODE = True

if __name__ == '__main__':
    # Start the rummy service
    subprocess.Popen('python captain.py', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    rummy_servers = discover('Rummy')
    rummy = rummy_servers[0]
    logger.info('Rummy running on {}:{}'.format(rummy[0], rummy[1]))
    # Connect to the rummy service
    c = rpyc.connect(rummy[0], rummy[1], config={"logger": logger})
    # c.modules.sys.stdout = sys.stdout
    # out = c.root.out
    # out(sys.stdout)
    # Set the netrefs
    wake = c.root.wake
    unlock = c.root.unlock
    gather = c.root.gather
    prepare = c.root.prepare
    add = c.root.add
    remove = c.root.remove
    ship_out = c.root.ship_out
    clues = c.root.clues
    verify = c.root.verify
    # Execute the correct netrefs and initialize all agents.
    logger.info('Waking the captain up...')
    msg = wake()
    logger.info(msg)
    logger.info('Unlocking all locks...')
    msg = unlock()
    logger.info(msg)
    if DEV_MODE:
        gather(1)
    else:
        gather()
    logger.info('Preparing for the journey...')
    msg = prepare()
    logger.info(msg)

    crew = add(psutil.cpu_count(logical=True))
    logger.info('Got crew: %s', crew)
    # pids = []
    # for crew_member in crew['data']:
    #     logger.info('Signing up {}'.format(crew_member))
    #     pid = subprocess.Popen('python pirate.py', stdout=subprocess.PIPE)
    #     pids.append(pid)

    pirate_servers = discover('Pirate')
    logger.info('All pirates: {}'.format(pirate_servers))
    for server in pirate_servers:
        logger.info('Connecting to {}:{}'.format(server[0], server[1]))
        conn = rpyc.connect(server[0], server[1])
        set_id = conn.root.set_id
        set_id(crew['data'][pirate_servers.index(server)])
    first_mate = rpyc.connect(pirate_servers[0][0], pirate_servers[0][1])
    elect_leader = first_mate.root.elect_leader
    leader_details = elect_leader()
    logger.info('Leader: {}'.format(str(leader_details)))
    leader = rpyc.connect(leader_details[0], leader_details[1])
    start = rpyc.async(leader.root.start)
    ship_out()
    start()
    # for pid in pids:
    #     os.kill(pid)

    # num_pirates = psutil.cpu_count(logical=True)
    # with open('data/clues/1.map', 'r') as file:
    #     clue = file.read()
    #     servers = discover('Pirate')
    #     logger.info('Discovered servers: %s', servers)
    #     connections = []
    #     for server in servers:
    #         host, port = server
    #         logger.info('Connecting to {host}:{port}'.format(host=host, port=port))
    #         connections.append(rpyc.connect(host, port))
    #     results = []
    #     for c in connections:
    #         logger.info('Sending clue...')
    #         request = async(c.root.solve)
    #         result = request(clue)
    #         # result.add_callback(log)
    #         results.append(result)
    #     for r in results:
    #         r.wait()
