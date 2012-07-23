'''
tracker.py - global system tracker class
'''

import socket
import communication
import db
from peer import LocalPeer
import logging
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
   
    def __init__(self, port=PORT):
        # add all local files to the state data
        # TODO
        super(Tracker, self).__init__(hostname=Tracker.HOSTNAME, port=port)
        self.db = db.TrackerDb()
        self.start_accepting_connections()

    def handle_CONNECT_REQUEST(self, client_socket, msg):
        logging.debug("Handling a connect request message")
        response = messages.ConnectResponse(successful=False)
        
        if msg.pwd == LocalPeer.PASSWORD:
            response.successful = True
        
        peer_endpoint = client_socket.getpeername()
        port = client_socket.getsockaddress[1]
        self.db.add_peer(peer_endpoint, port, PeerState.ONLINE, 
        
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
    
    
