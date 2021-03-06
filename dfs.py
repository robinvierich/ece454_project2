'''
dfs.py: Distributed File System

Provides the interface to the distributed file system.
An entry point of dfs.
'''

from tracker import Tracker
from peer import LocalPeer
from optparse import OptionParser
import logging
import sys
import re
import os.path
import filesystem

local_peer = None

def init_local_peer(tracker_hostname, tracker_port, self_ip, root_path=None, db_name=None):
    global local_peer
    Tracker.HOSTNAME = tracker_hostname
    Tracker.PORT = tracker_port
    
    if root_path:
        local_peer = LocalPeer(hostname=self_ip, root_path=root_path, db_name=db_name)
    else:
        local_peer = LocalPeer(hostname=self_ip, db_name=db_name)

def init_tracker(tracker_port, self_ip):
    global local_peer
    local_peer = Tracker(port=tracker_port, hostname=self_ip)

# Connection
def connect(password):
    return local_peer.connect(password)


def disconnect(check_for_unreplicated_files=True):
    return local_peer.disconnect(check_for_unreplicated_files)

def add_file(src_file_path, dest_file_path):
    return local_peer.add_file(src_file_path, dest_file_path)

# File Operations
def read(file_path, start_offset=None, length=None):
    # query the tracker for the peers with this file_path
    return local_peer.read(file_path, start_offset, length)


def write(file_path, new_data, start_offset=None):
    return local_peer.write(file_path, new_data, start_offset)

def delete(file_path):
    return local_peer.delete(file_path)

def move(src_path, dest_path):
    return local_peer.move(src_path, dest_path)

def ls(dir_path=None):
    return local_peer.ls(dir_path)

def archive(file_path=None):
    return local_peer.archive(file_path)


# CLI Helper functions

def write_cli(path):
    logging.debug("Asking the local peer to write")
    data = filesystem.read_file(path)
    local_peer.write(path, data)


def main():
    parser = OptionParser()
    parser.add_option("-t", "--tracker", action="store_true", dest="tracker",
                      help="Start a tracker on this system.")
    parser.add_option("-c", "--connect", action="store_false", dest="tracker",
                      help="Connect to a tracker.")
    parser.add_option("-i", "--ip", action="store", dest="ip", 
                      help="IP address of the tracker to connect to.")
    parser.add_option("-s", "--self_ip", action="store", dest="self_ip", 
                      help="External IP address of this peer.")
    parser.add_option("-p", "--port", action="store", dest="port",
                      help="Start a tracker on the current system.")
    parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
                      help='Enable verbose output.')

    (options, args) = parser.parse_args()
    
    logging_level = logging.DEBUG
    logging.basicConfig(level=logging.DEBUG, 
                        format="%(filename)s::%(funcName)s()::%(lineno)d (%(threadName)s)   %(asctime)s\n\t>> %(message)s")
    # handle the command line arguments
    if options.verbose is None:
        logging.disable(logging.CRITICAL)

    if options.tracker:
        # iniitialize the tracker
        if options.port is None or options.self_ip is None:
            print "You must specify the tracker's port, as well as its external ip."
            sys.exit()
        init_tracker(int(options.port), options.self_ip)
    else:
        # initialize the local peer
        if options.port is None or options.ip is None or options.self_ip is None:
            print "You must specify the IP and port of the tracker to connect to as well as the external ip of this peer."
            sys.exit()
        init_local_peer(options.ip, int(options.port), options.self_ip)
    
    # Very simple cli
    while(True):
        inp = raw_input("> ")
        if re.match(r'write', inp):
            m = re.search(r'\s[^\s]+', inp)
            if m is None:
                print "You must enter a file name"
                continue
            f = re.search(r'[^\s]+', m.group()).group()
            if not os.path.exists(f):
                print "File doesn't exist"
                continue
            write_cli(f)
        elif re.match(r'disco', inp):
            local_peer.disconnect()
        elif re.match(r'conn', inp):
            local_peer.connect(LocalPeer.PASSWORD)
        elif re.match(r'quit', inp):
            sys.exit()
        elif re.match(r'ls', inp):
            for f in local_peer.ls():
                print "%s\n\tSize: %d LastVer: %d" % (f.path, f.size, f.latest_version)
        elif re.match(r'arch', inp):
            m = re.search(r'\s[^\s]+', inp)
            if m is None:
                print "You must enter a file name"
                continue
            f = re.search(r'[^\s]+', m.group()).group()
            print str(local_peer.archive(f))


# tests
if __name__ == "__main__":
    #import dfs_test
    #dfs_test.run()
    
    main()
