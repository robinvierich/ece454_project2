'''
tracker.py - global system tracker class
'''

import socket
import communication
import db
from peer import Peer, LocalPeer

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
        port_int = int(port)
        super(Tracker, self).__init__(hostname=Tracker.HOSTNAME, port=port_int)
        self.db = db.TrackerDb()
        self.start_accepting_connections()

    def handle_CONNECT_REQUEST(self, client_socket, msg):
        response = messages.ConnectResponse(successful=False)
        
        if msg.pwd == "test":
            response.successful = True
        
        #TODO: ensure getpeername() returns a unique identifier
        peer_endpoint = client_socket.getpeername()
        peer = Peer(peer_endpoint[0], peer_endpoint[1])
        
        connected_peers[peer] = None # add to connected peers index
        
        communication.send_message(response, socket=client_socket)
    
    @check_connected
    def handle_DISCONNECT_REQUEST(self, client_socket, disconnect_request):
        if not disconnect_request.check_for_unreplicated_files:
            response = messages.DisconnectResponse(False)
            communication.send_message(response, socket=client_socket)
            return
    
        # TODO: check if there are unreplicated files on the node
        unreplicated = False
        response = messages.DisconnectResponse(unreplicated)
        communication.send_message(response, socket=client_socket)
    
    @check_connected
    def handle_PEER_LIST_REQUEST(self, client_socket, peer_list_request):
        file_path = peer_list_request.file_path
                
        # TODO: use DB to get connected peers + filter by the file_path
        peer_list = connected_peers.keys()

        response = messages.PeerList(peer_list)
        communication.send_message(response, socket=client_socket)
    
    @check_connected
    def handle_NEW_FILE_AVAILABLE(self, client_socket, msg):
        # TODO: add file to DB + choose peers where it should be replicated
        
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
    
    
