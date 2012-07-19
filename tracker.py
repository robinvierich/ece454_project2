'''
tracker.py - global system tracker class
'''

import socket
import communication
from peer import LocalPeer

import messages

# this will be in DB later
connected_peers = {}

def check_connected(function):
    """A decorator that checks if the requesting peer is connected 
        before running a function. 
            - returns if not connected """

    def wrapper(*args, **kwargs):
        
        peer_socket = args[0]
        peer_endpoint = peer_socket.getpeername()
        
        if not connected_peers.has_key(peer_endpoint):
            return
            
        return_value = function(*args, **kwargs)
        return return_value
            
        
    return wrapper




class Tracker(LocalPeer):
    
    HOSTNAME = "localhost"
    PORT = 12345
   
    def __init__(self):
        super(Tracker, self).__init__(hostname=Tracker.HOSTNAME, port=Tracker.PORT)
    
    def handle_CONNECT_REQUEST(self, client_socket, msg):
        response = messages.ConnectResponse(successful=False)
        
        if msg.pwd == "test":
            response.successful = True
        
        peer_endpoint = client_socket.getpeername()
        connected_peers[peer_endpoint] = None
        
        communication.send_message(response, socket=client_socket)
    
    @check_connected
    def handle_DISCONNECT_REQUEST(self, client_socket, msg):
        pass
    
    @check_connected
    def handle_PEER_LIST_REQUEST(self, client_socket, msg):
        pass
    
    @check_connected
    def handle_NEW_FILE_AVAILABLE(self, client_socket, msg):
        pass
    
    @check_connected
    def handle_VALIDATE_CHECKSUM_REQUEST(self, client_socket, msg):
        pass
    
    @check_connected
    def handle_DELETE_REQUEST(self, client_socket, msg):
        pass
    
    @check_connected
    def handle_DELETE(self, client_socket, msg):
        pass

    @check_connected    
    def handle_ARCHIVE_REQUEST(self, client_socket, msg):
        pass
    
    
