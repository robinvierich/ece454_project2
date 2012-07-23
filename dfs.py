'''
dfs.py: Distributed File System

Provides the interface to the distributed file system.
An entry point of dfs.
'''

import socket
import communication
import messages
import checksum
import filesystem
from tracker import Tracker
from peer import Peer, LocalPeer
from optparse import OptionParser
import logging
import sys
import re
import os.path

local_peer = LocalPeer()

def init_local_peer(tracker_hostname, tracker_port):
    global local_peer
    Tracker.HOSTNAME = tracker_hostname
    Tracker.PORT = tracker_port

def init_tracker(tracker_port):
    global local_peer
    local_peer = Tracker(port=tracker_port)

# Connection
def connect(password):
    local_peer.connect(password)
    

def disconnect(check_for_unreplicated_files=True):
    local_peer.disconnect(check_for_unreplicated_files)


# File Operations
def read(file_path, start_offset=None, length=None):
    # query the tracker for the peers with this file_path
    local_peer.read(file_path, start_offset, length)


def write(file_path, new_data, start_offset=None):
    local_peer.write(file_path, new_data, start_offset)

def delete(file_path):
    local_peer.delete(file_path)

def move(src_path, dest_path):
    local_peer.move(src_path, dest_path)

def ls(dir_path=None):
    local_peer.ls(dir_path)

def archive(file_path=None):
    local_peer.archive(file_path)


def main():
    localPeer = None

    parser = OptionParser()
    parser.add_option("-t", "--tracker", action="store_true", dest="tracker",
                      help="Start a tracker on this system.")
    parser.add_option("-c", "--connect", action="store_false", dest="tracker",
                      help="Connect to a tracker.")
    parser.add_option("-i", "--ip", action="store", dest="ip", 
                      help="IP address of the tracker to connect to.")
    parser.add_option("-p", "--port", action="store", dest="port",
                      help="Start a tracker on the current system.")
    parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
                      help='Enable verbose output.')

    (options, args) = parser.parse_args()
    
    logging_level = logging.DEBUG
    logging.basicConfig(level=logging.DEBUG, 
                        format="%(threadName)s %(filename)s %(funcName)s: %(message)s")
    # handle the command line arguments
    if options.verbose is None:
        logging.disable(logging.CRITICAL)

    if options.tracker:
        # iniitialize the tracker
        if options.port is None:
            print "You must specify the tracker's port."
            sys.exit()
        tracker = Tracker(int(options.port))
        tracker.start_accepting_connections()
    else:
        # initialize the local peer
        if options.port is None or options.ip is None:
            print "You must specify the IP and port of the tracker to connect to."
            sys.exit()
        localPeer = LocalPeer()
        Tracker.HOSTNAME = options.ip
        Tracker.PORT = options.port
    
    #localPeer.start_accepting_connections()

    # Very simple cli
    while(True):
        inp = raw_input("> ")
        if re.match(r'add', inp):
            m = re.search(r'\s[^\s]+', inp)
            if m is None:
                print "You must enter a file name"
                continue
            f = re.search(r'[^\s]+', m.group()).group()
            if not os.path.exists(f):
                print "File doesn't exist"
                continue
            localPeer.add_new_file(f)
        elif re.match(r'^quit', inp):
            sys.exit()



# tests
if __name__ == "__main__":
    #import dfs_test
    #dfs_test.run()
    
    main()
