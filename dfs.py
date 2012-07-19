'''
dfs.py: Distributed File System

Provides the interface to the distributed file system.
'''

import socket
import communication
import messages
import checksum
import filesystem
from tracker import Tracker
from peer import Peer, LocalPeer

local_peer = LocalPeer()
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

# tests
if __name__ == "__main__":
    import dfs_test
    dfs_test.run()
    
