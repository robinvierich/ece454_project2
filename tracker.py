'''
tracker.py - global system tracker class
'''
import communication
import db

from peer import LocalPeer, PeerState
import logging
import messages
import peer
#from Cheetah.Templates._SkeletonPage import True


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
    REPLICATION_LEVEL = 100

    def __init__(self, port=PORT, hostname=HOSTNAME):
        Tracker.PORT = port
        
        super(Tracker, self).__init__(hostname, port)
        self.tracker = peer.Peer(self.hostname, self.port)
        self.db = db.TrackerDb()
        # add itself to the peers database
        self.db.add_peer(self.hostname, self.port, PeerState.ONLINE, 
                         LocalPeer.MAX_FILE_SIZE, LocalPeer.MAX_FILE_SYS_SIZE, 0, block=True)
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

        # TODO broadcast event
    
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

        # TODO broadcast event
    
    @check_connected
    def handle_PEER_LIST_REQUEST(self, client_socket, peer_list_request):
        logging.debug("Handling the peer list request")
        file_path = peer_list_request.file_path
        
        peer_list = []
        try:
            peer_list = self.db.get_peers(file_path)
        except RuntimeError, e:
            logging.debug("couldn't get peer list: " + str(e))
    
        logging.debug("Sending the peer list:\n%s" % [str(p) for p in peer_list])

        response = messages.PeerList(peer_list)
        communication.send_message(response, socket=client_socket)
    
    @check_connected
    def handle_NEW_FILE_AVAILABLE(self, client_socket, new_file_available_msg):
        logging.debug("Handling new file available message")
        f = new_file_available_msg.file_model
        peer_ip = client_socket.getpeername()[0]
        peer_port = new_file_available_msg.port

        if peer_ip == self.hostname and peer_port == self.port:
            return
                
        self.db.add_or_update_file(f)
        self.db.add_file_peer_entry(f, peer_ip, peer_port)
        
        # for now, to find peers where the file should be replicated, 
        # just select peers that are able to replicate the file and 
        # that have the smallest fs size. limit the # by replication level

        peers_list = self.db.get_peers_to_replicate_file(f, peer_ip, peer_port, 
                                                         Tracker.REPLICATION_LEVEL)

        # broadcast
        logging.debug("Broadcasting message ")
        for i in peers_list:            
            # TODO handle the tracker's case
            if i[1] == self.hostname and i[2] == self.port:
                self._download_file(f.path)
                continue
            logging.debug("Broadcasting to peer " + str(i[1]) + " " + str(i[2]))
            p = peer.Peer(i[1], i[2])            
            communication.send_message(new_file_available_msg, p)
            
    # this either means that a peer now has this particular file
    # or the file has actually been changed
    def handle_FILE_CHANGED(self, client_socket, file_changed_msg):
        remote_file = file_changed_msg.file_model

        peer_ip = client_socket.getpeername()[0]
        peer_port = file_changed_msg.port

        if peer_ip == self.hostname and peer_port == self.port:
            return

        db_file = self.db.get_file(remote_file.path)
        # if checksums match, that means the file wasn't actually updated
        # but the peer just downloaded it.
        if (db_file.checksum == remote_file.checksum and
            db_file.latest_version == remote_file.latest_version):
            
            logging.debug("A peer now has file " + remote_file.path)
            self.db.add_file_peer_entry(remote_file, peer_ip, peer_port)
        else:
            # this is a file change. notify all peers that have the file
            logging.debug("Peer's file (%s) was change. Going to notify all peers", 
                          remote_file.path)
            

        # if the tracker has the file himself, update it locally as well
        if self.db.peer_has_file(remote_file.path, self.hostname, self.port):
            LocalPeer.handle_FILE_CHANGED(self, client_socket, file_changed_msg)
            

    
    @check_connected
    def handle_LIST_REQUEST(self, client_socket, list_request):
        logging.debug("Handling list request")
        file_list = self.db.list_files(list_request.dir_path)

        list_response = messages.List(file_list)
        communication.send_message(list_response, socket=client_socket)
    
    @check_connected
    def handle_VALIDATE_CHECKSUM_REQUEST(self, client_socket, msg):
        logging.debug("Handling the check checksum request")
        valid = self.db.check_checksum(msg.file_path, msg.file_checksum)
        response = messages.ValidateChecksumResponse(msg.file_path, valid)
        communication.send_message(response, socket=client_socket)
    
    @check_connected
    def handle_DELETE_REQUEST(self, client_socket, msg):
        pass
    
    @check_connected
    def handle_DELETE(self, client_socket, msg):
        pass

    @check_connected    
    def handle_ARCHIVE_REQUEST(self, client_socket, archive_request):
        file_path = archive_request.file_path
        response = messages.ArchiveResponse(file_path, archived=False)
        
        logging.info("Handling Archive Request")
        
        f = self.db.get_file(file_path)
        if not f:
            communication.send_message(response, socket=client_socket)
            return
        
        f.latest_version += 1
        self.db.add_or_update_file(f)
        
        response.archived = True
        communication.send_message(response, socket=client_socket)
                
        peers_list = self.db.get_peers(file_path)
        
        # notify all peers that have the file about the new version
        for peer in peers_list:
            file_archived_msg = messages.FileArchived(f.path, f.latest_version)
            communication.send_message(file_archived_msg, peer)
            
    
