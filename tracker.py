'''
tracker.py - global system tracker class
'''
import communication
import db

from peer import LocalPeer, PeerState
import logging
import messages
import peer


def check_connected(function):
    """A decorator that checks if the requesting peer is connected 
        before running a function. 
            - returns if not connected """

    def wrapper(*args, **kwargs):
        tracker = args[0]
        peer_socket = args[1]
        peer_endpoint = peer_socket.getpeername()
        
        peer_hostname = peer_endpoint[0]
        
        if tracker.db.get_peer_state(peer_hostname) != PeerState.ONLINE:
            return
            
        return_value = function(*args, **kwargs)
        return return_value
        
    return wrapper

class Tracker(LocalPeer):
    
    HOSTNAME = "localhost"
    PORT = 12345
   
    def __init__(self, port=PORT):
        super(Tracker, self).__init__(hostname=Tracker.HOSTNAME, port=port)
        self.db = db.TrackerDb()
        self.start_accepting_connections()

    def handle_CONNECT_REQUEST(self, client_socket, msg):        
        response = messages.ConnectResponse(successful=False)
        
        if msg.pwd == LocalPeer.PASSWORD:
            logging.debug("Connection request - password OK")
            response.successful = True

            peer_endpoint = client_socket.getpeername()
            logging.debug("Peer address: " + str(peer_endpoint[0]) + " " + str(msg.port)) 
            self.db.add_peer(peer_endpoint[0], msg.port, peer.PeerState.ONLINE, msg.maxFileSize,
                             msg.maxFileSysSize, msg.currFileSysSize)
            
        else:
            logging.debug("Connection Request - wrong password")        

        communication.send_message(response, socket=client_socket)
    
    @check_connected
    def handle_DISCONNECT_REQUEST(self, client_socket, disconnect_request):
        logging.debug("Handling disconnect request")
        if not disconnect_request.check_for_unreplicated_files:
            response = messages.DisconnectResponse(False)
            communication.send_message(response, socket=client_socket)
            return
    
        peer_ip = client_socket.getpeername()[0]
        peer_port = disconnect_request.port
        unreplicated = self.db.has_unreplicated_files(peer_ip, peer_port)
        logging.debug("Unreplicated files: " + str(unreplicated))
        response = messages.DisconnectResponse(unreplicated)
        communication.send_message(response, socket=client_socket)
    
    @check_connected
    def handle_PEER_LIST_REQUEST(self, client_socket, peer_list_request):
        logging.debug("Handling the peer list request")
        file_path = peer_list_request.file_path
                    
        peer_list = self.db.get_peer_list(file_path)        
        logging.debug("Sending the peer list:\n" + str(peer_list))

        response = messages.PeerList(peer_list)
        communication.send_message(response, socket=client_socket)
    
    @check_connected
    def handle_NEW_FILE_AVAILABLE(self, client_socket, new_file_available_msg):
        # TODO: peers where file should be replicated
        f = new_file_available_msg.file_model
        
        file_path = f.file_path
        is_directory = f.is_directory
        checksum = f.checksum
        size = f.size
        latest_version = f.latest_version
        
        self.db.add_file(file_path, is_directory, size, checksum, latest_version)
    
    @check_connected
    def handle_LIST_REQUEST(self, client_socket, list_request):
        logging.debug("Handling list request")
        file_list = self.db.list_files(list_request.dir_path)

        list_response = messages.List(file_list)
        communication.send_message(list_response, socket=client_socket)
    
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
    
    
