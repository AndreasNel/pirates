import psutil
import rpyc
from rpyc.utils.factory import discover
import logging
import subprocess

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s: (%(name)s) %(levelname)s - %(message)s')
ch.setFormatter(formatter)
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
    # if DEV_MODE:
    #     gather(1)
    # else:
    #     gather()
    logger.info('Preparing for the journey...')
    msg = prepare()
    logger.info(msg)

    pirate_servers = discover('Pirate')
    crew = add(len(pirate_servers))
    logger.info('Got crew: %s', crew)
    ship_out()

    logger.info('All pirates: {}'.format(pirate_servers))
    for server in pirate_servers:
        logger.info('Connecting to {}:{}'.format(server[0], server[1]))
        conn = rpyc.connect(server[0], server[1])
        set_id = conn.root.set_id
        set_id(crew['data'][pirate_servers.index(server)])
    random_pirate = rpyc.connect(pirate_servers[0][0], pirate_servers[0][1])
    elect_leader = random_pirate.root.elect_leader
    leader_details = elect_leader()
    logger.info('Leader: {}'.format(str(leader_details)))
    leader = rpyc.connect(leader_details[0], leader_details[1])
    start = rpyc.async(leader.root.start)
    start()
