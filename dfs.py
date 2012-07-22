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

#local_peer = LocalPeer()
peer_socket_index = {}

tracker = Peer(Tracker.HOSTNAME, Tracker.PORT)

# Connection
def connect(password):
    
    connect_request = messages.ConnectRequest(password)
    # Send Connection Request to Tracker
    communication.send_message(connect_request, tracker)
    response = communication.recv_message(tracker)
    
    successful = response.successful
    if successful:
        local_peer.start_accepting_connections()
        
    return successful

def disconnect(check_for_unreplicated_files=True):
    
    communication.send_message(messages.DisconnectRequest(), tracker)
    response = communication.recv_message(tracker) # blocks
    
    while (response.should_wait):
        communication.send_message(messages.DisconnectRequest(), tracker)
        response = communication.recv_message(tracker) # blocks    
    
    
    local_peer.stop()


# File Operations

def read(file_path, start_offset=None, length=None):
    
    # query the tracker for the peers with this file_path
    
    
    pass


def write(file_path, new_data, start_offset=None):
    filesystem.write_file(file_path, new_data, start_offset)
    
    data = filesystem.read_file(file_path)
    new_checksum = checksum.calc_checksum(data)
    
    peer_list_request = messages.PeerListRequest(file_path)
    communication.send_message(peer_list_request, tracker)
    
    peer_list_response = communication.recv_message(tracker)
    peer_list = peer_list_response.peer_list
    
    for peer in peer_list:
        file_changed_msg = messages.FileChanged(file_path, new_checksum, new_data, start_offset)
        communication.send_message(file_changed_msg, peer)

def delete(file_path):
    pass

def move(src_path, dest_path):
    pass

def ls(dir_path=None):
    pass

def archive(file_path=None):
    pass


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
        localPeer = Tracker(int(options.port))
    else:
        # initialize the local peer
        if options.port is None or options.ip is None:
            print "You must specify the IP and port of the tracker to connect to."
            sys.exit()
        localPeer = LocalPeer()
        Tracker.HOSTNAME = options.ip
        Tracker.PORT = options.port
    
    localPeer.start_accepting_connections()

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
