'''
Peer.py - Remote peer model
'''

import socket
import threading
import os.path
import logging
import select

import communication
from messages import MessageType, FileModel
import messages
import checksum
import filesystem
import tracker
from db import LocalPeerDb

class PeerState(object):
    ONLINE = 1
    OFFLINE = 2
    

class Peer(object):
    HOSTNAME = "127.0.0.1"
    PORT = 11111
    NAME = "Peer"
    
    def __init__(self, hostname=HOSTNAME, port=PORT, name=NAME, state=PeerState.OFFLINE):
        self.hostname = hostname
        self.port = port
        self.name = name
        self.state = state
    
    def __str__(self):
        return "Peer. ip=%s port=%s" % (self.hostname, self.port)

def check_tracker_online(function):
    """A decorator that checks if the tracker is online
        before running a function. 
        
        If the tracker is not connected, <function_name>_tracker_offline is called
        """

    def wrapper(*args, **kwargs):
        peer = args[0]
        
        function_to_call = function
        
        if peer.is_tracker_online() == False:
            function_to_call = getattr(peer, function.func_name + '_tracker_offline')
        
        if function_to_call == None:
            return
            
        return_value = function_to_call(*args, **kwargs)
        return return_value
        
    return wrapper


class LocalPeer(Peer):  
    LOCAL_STORE = "dfs"
    PASSWORD = '12345'
    MAX_FILE_SIZE = 100000000
    MAX_FILE_SYS_SIZE = 1000000000
    def __init__(self, hostname=Peer.HOSTNAME, port=Peer.PORT, root_path=LOCAL_STORE, db_name=None):
        super(LocalPeer, self).__init__(hostname, port)        
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._acceptorThread = AcceptorThread(self)
        self.start_server()
        
        self._backlog = []
        
        self.root_path = root_path
        

        if type(self) == LocalPeer: # exclude sub-classes
            self.tracker = Peer(tracker.Tracker.HOSTNAME, tracker.Tracker.PORT)
            self.db = LocalPeerDb(db_name)
            self.connect(LocalPeer.PASSWORD)            

    def start_server(self):        
        connected = False
        while not connected:
            try:
                self._server_socket.bind((self.hostname, self.port))
                self._server_socket.listen(5)
                connected = True
                logging.info("Server started. Listening on port %i" % self.port)
            except:
                logging.error("Couldn't listen on port %i. Trying %i" % (self.port, self.port + 1))
                self.port += 1
        
    def connect(self, password):
        connect_request = messages.ConnectRequest(password, self.port, LocalPeer.MAX_FILE_SIZE,
                                                  LocalPeer.MAX_FILE_SYS_SIZE, 0)
        # Send Connection Request to Tracker
        communication.send_message(connect_request, self.tracker)
        response = communication.recv_message(self.tracker)
        
        successful = response.successful
        if successful:
            logging.info("%s : Connection to tracker successful" % self)
            self.start_accepting_connections()
            self.state = PeerState.ONLINE
            # get peers and file lists
            peer_list = self._get_peer_list(None)
            self.db.clear_peers_and_insert(peer_list)
            
            file_list = self.ls()
            for remote_file in file_list:
                # for each file, check locally stored version and checksum
                # and fetch changes, as needed
                db_file = self.db.get_file(remote_file.path)
                if db_file is None:
                    logging.debug("New file: %s", remote_file.path)
                    self.db.add_or_update_file(remote_file)
                else:
                    print "1"
                    if remote_file.latest_version == db_file.latest_version:
                        if remote_file.checksum != db_file.checksum:
                            # same version, but file is changed. re-fetch
                            self._download_file(remote_file.path) # check if successful
                    else:
                        # TODO fetch the new versions
                        pass
        else:
            logging.error("%s : Connection to tracker unsuccessful" % self)
                
        return successful

    def disconnect(self,check_for_unreplicated_files=True):
        logging.info("Asking tracker to disconnect")
        disconnect_msg = messages.DisconnectRequest(check_for_unreplicated_files, self.port)
        communication.send_message(disconnect_msg, self.tracker)
        response = communication.recv_message(self.tracker)
        logging.info("Response received. Should wait? " + str(response.should_wait))
        while (response.should_wait):
            # TODO so is it is a tracker's responsibility to notify peer when it is ok to disconnect?
            response = communication.recv_message(self.tracker)

        self.stop()
    
    def _download_file(self, file_path, peer_list=None, maxAttempts=3):
        # get peer list for this file_path
        if peer_list == None:
            peer_list = self._get_peer_list(file_path)
        
        attempt = 0
        while attempt < maxAttempts:
            response = None
            # download the file from a peer
            for peer in peer_list:
                file_download_request = messages.FileDownloadRequest(file_path)
                communication.send_message(file_download_request, peer)
                
                response = communication.recv_message(peer)
                if isinstance(response, messages.FileData):
                    break
            
            if response == None or isinstance(response, messages.FileDownloadDecline):
                return None
            
            f = response.file_model
            version = f.latest_version
            golden_checksum = f.checksum
            local_path = filesystem.get_local_path(self, file_path, version)
            
            filesystem.write_file(local_path, f.data)
            new_checksum = checksum.calc_file_checksum(local_path)
        
            if new_checksum == golden_checksum:
                logging.info("File downloaded successfully! Going to notify tracker. File name: " + f.path)
                
                self.db.add_or_update_file(f)
                self.db.add_local_file(f.path)
                f.data = None
                response = messages.FileChanged(f, self.port)
                communication.send_message(response, self.tracker)
                return True
                
            else:
                logging.error("File downloaded but checksum doesn't match. " +
                              "Going to try again. File name: %s" % f.path)
                attempt += 1

        logging.error("Download_file failed - max attempts reached. File: " + file_path)
        return False
    
    # File Operations
    def is_tracker_online(self):
        try:
            communication.connect_to_peer(self.tracker)
            return True
        except socket.error, err:
            logging.error("Couldn't connect to tracker" + err)
            return False
        
    
    @check_tracker_online
    def read(self, file_path, start_offset=None, length=-1):
        local_path = filesystem.get_local_path(self, file_path)
        
        file_data = filesystem.read_file(local_path, start_offset, length)
        if file_data != None:
            return file_data
        
        self._download_file(file_path)
        file_data = filesystem.read_file(file_path, start_offset, length)
        
        return file_data
    
    def read_tracker_offline(self, file_path, start_offset=None, length=-1):
        file_data = filesystem.read_file(file_path, start_offset, length)
        if file_data != None:
            return file_data
        
        peer_list = self.db.get_peers()
        
        self._download_file(file_path, peer_list)
        
        pass
    
    @check_tracker_online
    def write(self, file_path, new_data, start_offset=None):
        f = self.db.get_file(file_path)
        
        if f is not None:
            is_new_file = False
            local_path = filesystem.get_local_path(self, file_path, f.latest_version)
        else:
            is_new_file = True
            local_path = filesystem.get_local_path(self, file_path)
        
        logging.info("Writing file %s to %s. New file? - %s", 
                        file_path, local_path, is_new_file)
        
        filesystem.write_file(local_path, new_data, start_offset)
        
        is_directory = False
        new_checksum = checksum.calc_file_checksum(local_path)
        new_size = os.path.getsize(local_path)
        new_data = filesystem.read_file(local_path) # we have to read here in case there was an offset

        file_model = FileModel(file_path, 
                               is_directory,
                               new_checksum,
                               new_size,
                               latest_version=1,
                               data=None)
        self.db.add_or_update_file(file_model)
        file_model.data = new_data
            
        if is_new_file:                                                
            file_msg = messages.NewFileAvailable(file_model, self.port)
        else:
            self.db.add_or_update_file(f)
            self.db.add_local_file(f.path)
            f.checksum = new_checksum
            f.size = new_size
            file_msg = messages.FileChanged(file_model, self.port, start_offset)
        
        communication.send_message(file_msg, self.tracker) # let the tracker know about the file

        # I believe that the following should be done by the tracker
        # Only do this if the tracker is offline

        # get peer list
        #peer_list = self._get_peer_list(file_path)
        
        #for peer in peer_list:
        #    communication.send_message(file_msg, peer)
        
    def _write_tracker_offline(self):
        
        #record the write in a backlog
        
        # send out newfile/filechanged messages to all peers
        
        pass
    
    def delete(self, file_path):
        delete_request = messages.DeleteRequest(file_path)
        communication.send_message(delete_request, self.tracker)
        delete_response = communication.recv_message(self.tracker)
        
        if not delete_response.can_delete:
            return False
        
        if (os.path.exists(file_path)):
            os.remove(file_path)
        
        peer_list = self._get_peer_list(file_path)
        
        delete_msg = messages.Delete(file_path)
        for peer in peer_list:
            communication.send_message(delete_msg, peer)
            
        return True
    
    def move(self, src_path, dest_path):
        move_request = messages.MoveRequest(src_path, dest_path)
        communication.send_message(move_request, self.tracker) 
        move_response = communication.recv_message(self.tracker)
        
        if not move_response.valid:
            return False
        
        filesystem.move(src_path, dest_path)
        
        peer_list = self._get_peer_list(src_path)
        move_msg = messages.Move(src_path, dest_path)
        for peer in peer_list:
            communication.send_message(move_msg, peer, socket)
        
        return True
    
    def ls(self,dir_path=None):
        list_request = messages.ListRequest(dir_path)
        communication.send_message(list_request, self.tracker)
        list_response = communication.recv_message(self.tracker)
        
        return list_response.file_list
        
    
    def archive(self,file_path):
        archive_request = messages.ArchiveRequest(file_path)
        communication.send_message(archive_request, self.tracker)
        archive_response = communication.recv_message(self.tracker)
        
        archived = archive_response.archived
        
        if not archived:
            return False
        
        f = self.db.get_file(file_path)
        if not f:
            return False
        
        local_file_path = filesystem.get_local_path(self, file_path, f.latest_version)
        file_data = filesystem.read_file(local_file_path)
        
        f.latest_version += 1
        self.db.add_or_update_file(f)
        
        local_file_path = filesystem.get_local_path(self, file_path, f.latest_version)
        filesystem.write_file(local_file_path, file_data)
        
        return True
        
    
    def start_accepting_connections(self):
        if self._acceptorThread.is_alive():
            return
        
        self._acceptorThread.start()
    
    def stop(self):
        print "Please wait. Stopping Incomming connections..."
        self._acceptorThread.stop = True
        self._acceptorThread.join(1)
        # force terminate the thread
        if self._acceptorThread.isAlive():
            print "Timed out. Try again later."
        else:
            self._acceptorThread = AcceptorThread(self)
    
    def get_server_socket(self):
        return self._server_socket

    def add_file_to_db(self, path):
        logging.info("Adding a new file to db: " + path)

        isDir = 1 if os.path.isdir(path) else 0
        size = os.path.getsize(path)
        cs = checksum.calc_file_checksum(path)
        
        file_model = FileModel(path, isDir, size, cs, 0)
        self.db.add_or_update_file(file_model)
    
    def add_file(self, src_file_path, dest_file_path):
        logging.info("Adding file to system")
        file = open(src_file_path, "r")
        data = file.read() 
        file.close()
        
        self.write(dest_file_path, data)

    #TODO: Probably makes more sense to make all these functions part of the peer class
    
    def _get_peer_list(self, file_path):
        logging.info("Requesting a peers list")
        peer_list_request = messages.PeerListRequest(file_path)
        communication.send_message(peer_list_request, self.tracker)
        
        peer_list_response = communication.recv_message(self.tracker)
        peer_list = peer_list_response.peer_list
        logging.info("Received a peers list:\n%s" % [str(p) for p in peer_list])
        return peer_list
    
    def _file_model_from_file(self, f):
        isDir = 1 if os.path.isdir(f) else 0
        size = os.path.getsize(f)
        cs = checksum.calc_file_checksum(f)
        
        file_model = FileModel(f, isDir, size, cs, 0)

    def get_handler_method_index(self):
        return {MessageType.ARCHIVE_REQUEST : self.handle_ARCHIVE_REQUEST,
                MessageType.PEER_LIST_REQUEST : self.handle_PEER_LIST_REQUEST,
                MessageType.PEER_LIST : self.handle_PEER_LIST,
                MessageType.FILE_DOWNLOAD_REQUEST : self.handle_FILE_DOWNLOAD_REQUEST,
                MessageType.FILE_DOWNLOAD_DECLINE : self.handle_FILE_DOWNLOAD_DECLINE,
                MessageType.FILE_DATA : self.handle_FILE_DATA,
                
                MessageType.CONNECT_REQUEST : self.handle_CONNECT_REQUEST,
                MessageType.CONNECT_RESPONSE : self.handle_CONNECT_RESPONSE,
            
                MessageType.DISCONNECT_REQUEST : self.handle_DISCONNECT_REQUEST,
                MessageType.DISCONNECT_RESPONSE : self.handle_DISCONNECT_RESPONSE,
                
                MessageType.FILE_CHANGED : self.handle_FILE_CHANGED,
                MessageType.NEW_FILE_AVAILABLE : self.handle_NEW_FILE_AVAILABLE,
                
                MessageType.VALIDATE_CHECKSUM_REQUEST : self.handle_VALIDATE_CHECKSUM_REQUEST,
                MessageType.VALIDATE_CHECKSUM_RESPONSE : self.handle_VALIDATE_CHECKSUM_RESPONSE,
                
                MessageType.DELETE_REQUEST : self.handle_DELETE_REQUEST,
                MessageType.DELETE_RESPONSE : self.handle_DELETE_RESPONSE,
                MessageType.DELETE : self.handle_DELETE,
                
                MessageType.MOVE_REQUEST : self.handle_MOVE_REQUEST,
                MessageType.MOVE_RESPONSE : self.handle_MOVE_RESPONSE,
                MessageType.MOVE : self.handle_MOVE,
                
                MessageType.LIST_REQUEST : self.handle_LIST_REQUEST,
                MessageType.LIST : self.handle_LIST,
                
                MessageType.ARCHIVE_REQUEST : self.handle_ARCHIVE_REQUEST,
                MessageType.ARCHIVE_RESPONSE : self.handle_ARCHIVE_RESPONSE,
                MessageType.FILE_ARCHIVED : self.handle_FILE_ARCHIVED,
                }

    def handle_CONNECT_REQUEST(self, client_socket, msg):
        source_ip = client_socket.getpeername()[0]
        source_port = msg.port
        self.db.add_or_update_peer(source_ip, source_port, PeerState.ONLINE)
            
        
    # not used - tracker
    def handle_CONNECT_RESPONSE(self, client_socket, msg):
        pass

    def handle_DISCONNECT_REQUEST(self, client_socket, msg):
        source_ip = client_socket.getpeername()[0]
        source_port = msg.port
        self.db.update_peer_state(source_ip, source_port, PeerState.OFFLINE)

    
    # not used
    def handle_DISCONNECT_RESPONSE(self, client_socket, msg):
        pass
    
    # not used
    def handle_PEER_LIST_REQUEST(self, client_socket, msg):
        pass
    
    def handle_PEER_LIST(self, client_socket, peer_list_msg):
        peer_list = peer_list_msg.peer_list
        
        self.db.clear_peers_and_insert(peer_list)
        

    def handle_FILE_DOWNLOAD_REQUEST(self, client_socket, msg):        
        logging.info("Handling the file download request for file " + msg.file_path)
        # TODO Need to be aware of versioning here.
        local_path = filesystem.get_local_path(self, msg.file_path)
        file_data = filesystem.read_file(local_path)
        if file_data is None:
            response = messages.FileDownloadDecline(msg.file_path)
        else:
            fm = self.db.get_file(msg.file_path)
            fm.data = file_data
            response = messages.FileData(fm)
        communication.send_message(response, socket=client_socket)
    

    def handle_FILE_DOWNLOAD_DECLINE(self, client_socket, msg):
        pass
    
    
    def handle_FILE_DATA(self, client_socket, file_data_msg):
        logging.info("Received file data for file " + file_data_msg.file_model.path)
        logging.info("File data: " + file_data_msg.file_model.data)
        # save file
        f = file_data_msg.file_model
        start_offset = file_data_msg.start_offset
        
        local_path = filesystem.get_local_path(self, f.path, f.latest_version)
        filesystem.write_file(local_path, f.data, start_offset)
        
        # add file to db
        self.db.add_or_update_file(f)
        
        file_model = FileModel(f.path, f.is_dir, f.checksum, f.size, f.latest_version, f.data)
        new_file_available_msg = messages.NewFileAvailable(file_model)
        
        if self.is_tracker_online():
            communication.send_message(new_file_available_msg, self.tracker)
        else:
            self._backlog.append((new_file_available_msg, self.tracker))

    
    def handle_FILE_CHANGED(self, client_socket, file_changed_msg):        
        remote_file = file_changed_msg.file_model
        start_offset = file_changed_msg.start_offset

        # if the curr checksum is the same as remote, don't do anything
        db_file = self.db.get_file(remote_file.path)
        if db_file.checksum == remote_file.checksum:
            logging.debug("Received file change, but it appears that local file is up-to-date")
            return

        peer_ip = client_socket.getpeername()[0]
        peer_port = file_changed_msg.port

        local_path = filesystem.get_local_path(self, remote_file.path, remote_file.latest_version)

        if not os.path.exists(local_path):
            logging.warning("Recieved a file change message, but there is no such file locally")
            return
        
        filesystem.write_file(local_path, remote_file.data, start_offset)
        
        new_checksum = checksum.calc_file_checksum(local_path)
        
        if new_checksum == remote_file.checksum:
            logging.debug("File was updated. Notifying the tracker.")
            # notify tracker that peer now has updated file
            file_changed_msg.port = self.port
            communication.send_message(file_changed_msg, self.tracker)
            self.db.add_or_update_file(remote_file)
        else:
            logging.warning("Updated local file as per file changed message, but checksums don't " +
                            "match. Re-reqesting the file")
            communication.send_message(messages.FileDownloadRequest(remote_file.path), socket=client_socket)
    
    def handle_NEW_FILE_AVAILABLE(self, client_socket, msg):
        #self.db.add_or_update_file(msg.file_model)
        self._download_file(msg.file_model.path)

    def handle_VALIDATE_CHECKSUM_REQUEST(self, client_socket, msg):
        pass
    
    def handle_VALIDATE_CHECKSUM_RESPONSE(self, client_socket, msg):
        pass
    
    def handle_DELETE_REQUEST(self, client_socket, msg):
        pass
    
    def handle_DELETE_RESPONSE(self, client_socket, msg):
        pass
    
    def handle_DELETE(self, client_socket, msg):
        file_path = msg.file_path
        filesystem.delete_file(file_path)
    
    def handle_MOVE_REQUEST(self, client_socket, msg):
        pass
    def handle_MOVE_RESPONSE(self, client_socket, msg):
        pass
    
    def handle_MOVE(self, client_socket, msg):
        src_path = msg.src_path
        dest_path = msg.dest_path
        
        filesystem.move(src_path, dest_path)
    
    def handle_LIST_REQUEST(self, client_socket, msg):
        logging.debug("Handling list request")        
        file_list = self.db.list_files(msg.dir_path)
        response = messages.Message(messages.MessageType.LIST)
        response.file_list = file_list
        communication.send_message(response, socket=client_socket)
        
    def handle_LIST(self, client_socket, msg):
        pass
    def handle_ARCHIVE_REQUEST(self, client_socket, msg):        
        pass
    def handle_ARCHIVE_RESPONSE(self, client_socket, msg):
        pass
    
    def handle_FILE_ARCHIVED(self, client_socket, file_archived_msg):
        logging.info("Handling FILE_ARCHIVED message - creating new version of file")
        
        file_path = file_archived_msg.file_path
        new_version = file_archived_msg.new_version
        
        f = self.db.get_file(file_path)
        if not f:
            return
        if f.latest_version == new_version:
            return
        local_file_path = filesystem.get_local_path(self, file_path, f.latest_version)
        file_data = filesystem.read_file(local_file_path)
        
        f.latest_version = new_version
        self.db.add_or_update_file(f)
        
        local_file_path = filesystem.get_local_path(self, file_path, f.latest_version)
        filesystem.write_file(local_file_path, file_data)
    
class AcceptorThread(threading.Thread):
    def __init__(self, peer):
        super(AcceptorThread, self).__init__()
        self.name = "Acceptor"
        self._peer = peer
        self.stop = False

        self.alive = threading.Event()
        self.alive.set()
        # terminate this thread when the main thread exits
        self.daemon = True

    def run(self):        
        server_socket = self._peer.get_server_socket()
        while self.alive.is_set():            
            #logging.debug("Waiting for a connection")
            readable, writable, errored = select.select([server_socket], [], [], 0.5)
            if len(readable) != 0:                
                client_socket, addr = server_socket.accept()
                handler = HandlerThread(self._peer, client_socket)
                handler.start()
            if self.stop:                
                break
    
    def join(self, timeout=None):
        logging.debug("Ending thread")
        self.alive.clear()        
        threading.Thread.join(self, timeout)

class HandlerThread(threading.Thread):
    def __init__(self, peer, client_socket):            
        super(HandlerThread, self).__init__()
        self.daemon = False
        self.name = type(peer).__name__ + "_Handler"
        self._peer = peer
        self._client_socket = client_socket    
    
        
    def run(self):
        logging.debug("Spawned a HandlerThread")
        received_msg = communication.recv_message(socket=self._client_socket)

        msg_type = received_msg.msg_type
        logging.debug("Received " + str(received_msg))
        
        handler_method_index = self._peer.get_handler_method_index()
        
        handler_method = handler_method_index[msg_type]
        handler_method(self._client_socket, received_msg)
    
    def join(self, timeout=None):
        logging.debug("Ending thread")
        threading.Thread.join(self, timeout)

