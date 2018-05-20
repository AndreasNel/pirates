import rpyc
from rpyc.utils.factory import discover
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s: (main) %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

DEV_MODE = False

if __name__ == '__main__':
    # Start the quartermaster service
    qm_servers = discover('QuarterMaster')
    qm = qm_servers[0]
    logger.info('Quartermaster running on {}:{}'.format(qm[0], qm[1]))
    # Connect to the qm service
    c = rpyc.connect(qm[0], qm[1], config={"logger": logger})
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
    logger.info('Gathering clues...')
    if DEV_MODE:
        gather(1)
    else:
        gather()
    logger.info('Preparing for the journey...')
    msg = prepare()
    logger.info(msg)

    pirate_servers = discover('Pirate')
    if (len(pirate_servers) < 2):
        raise Exception('You cannot start this distributed system with less than two pirates to do the job.')
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
