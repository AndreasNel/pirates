# pirates
Project that aims to simulate a distributed computing system for the COS786 module at the University of Pretoria.

## Requirements
This project depends on the `rpyc` library. Run `pip3 install -r requirements.txt` in order to install the library. It has been developed to work on an Ubuntu distribution where the primary Python version is 3.6

## Executing the system
In order the correctly execute the system, the user should execute each of the following commands in a separate shell, in the specified order.

1. `python3 registry_server.py` - Starts up the Registry Server that tracks the different agents. This server should be started on a computer where the server is allowed to broadcast on the network (if everything is ran on the same computer, it should work by default).
2. `python3 quarter_master.py` - Starts the `QuarterMaster` service which is used to communicate with `rummy`. It is important that this file is in the same directory as the `rummy.pyc` file.
3. `python3 pirate.py` - Starts up a `Pirate` service. This command should be run in a separate shell for each pirate that the user requires. At least two instances of the `Pirate` service are required.
4. `python3 main.py` - Starts the entire process, starting with the waking of Rummy and the gathering of clues.
