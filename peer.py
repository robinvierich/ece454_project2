'''
Peer.py - Remote peer model
'''

import socket
import threading
import os.path
import logging
import random

import communication
from messages import MessageType
import messages
import checksum
import filesystem
import tracker
from db import PeerDb, LocalPeerDb, TrackerDb

import pdb

class Peer(object):
    HOSTNAME = "localhost"
    PORT = 11111
    
    def __init__(self, hostname=HOSTNAME, port=PORT):
        self.hostname = hostname
        self.port = port

class LocalPeer(Peer):  
    PASSWORD = '12345'
    def __init__(self, hostname=Peer.HOSTNAME, port=Peer.PORT):
        super(LocalPeer, self).__init__(hostname, port)
        
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._acceptorThread = AcceptorThread(self)
        self.start_server()
        
        if self.is_not_tracker():
            self.db = LocalPeerDb()
            # Connect to the tracker
            self.connect(LocalPeer.PASSWORD)
            self.tracker = Peer(tracker.Tracker.HOSTNAME, tracker.Tracker.PORT)

    def is_not_tracker(self):
        return not isinstance(self, tracker.Tracker)
    
    def start_server(self):        
        connected = False
        while not connected:
            try:
                self._server_socket.bind((self.hostname, self.port))
                self._server_socket.listen(5)
                connected = True
                logging.debug("Listening on port " + str(self.port))
            except:
                self.port += 1
    
#    def persist(self, (key, value)):
#        
#        # we'll add sql here, just a standard dict for now
#        self._persisted_data[key] = value
    
    def connect(self, password):
        connect_request = messages.ConnectRequest(password)
        # Send Connection Request to Tracker
        communication.send_message(connect_request, self.tracker)
        response = communication.recv_message(self.tracker)
        
        successful = response.successful
        if successful:
            self.start_accepting_connections()
        
        return successful

    def disconnect(self,check_for_unreplicated_files=True):
        communication.send_message(messages.DisconnectRequest(), self.tracker)
        response = communication.recv_message(self.tracker) # blocks
        
        while (response.should_wait):
            #communication.send_message(messages.DisconnectRequest(), tracker)
            response = communication.recv_message(self.tracker) # blocks    
        
        
        self.stop()
    
    def _download_file(self, file_path, maxAttempts=3):
        # get peer list for this file_path
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
            
            if response == None:
                return None
        
            filesystem.write_file(file_path, response.file_data)
            data = filesystem.read_file(file_path)
            new_checksum = checksum.calc_checksum(data)
        
            if new_checksum == response.file_checksum:
                return data
            else:
                attempt += 1
        
        raise Exception("download_file failed - max attempts reached")
    
    
    # File Operations
    def read(self, file_path, start_offset=None, length=-1):
        file_data = filesystem.read_file(file_path)
        if file_data != None:
            return file_data
        
        self._download_file(file_path)
        file_data = filesystem.read_file(file_path, start_offset, length)
        
        return file_data
    
    def write(self, file_path, new_data, start_offset=None):
        is_new_file = not os.path.exists(file_path)
        
        filesystem.write_file(file_path, new_data, start_offset)
        
        data = filesystem.read_file(file_path)
        new_checksum = checksum.calc_checksum(data)
        
        # get peer list
        peer_list = self._get_peer_list(file_path)
        
        if is_new_file:
            new_data = filesystem.read_file(file_path) # we have to read here in case there was an offset
            file_msg = messages.NewFileAvailable(file_path, new_checksum, new_data)
        else:
            file_msg = messages.FileChanged(file_path, new_checksum, new_data, start_offset)
        
        for peer in peer_list:
            communication.send_message(file_msg, peer)
        
    
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
        pass
    
    def archive(self,file_path=None):
        pass
    
    def start_accepting_connections(self):
        
        self._acceptorThread.start()
    
    def stop(self):
        self._acceptorThread.join(1)
    
    def get_server_socket(self):
        return self._server_socket

    def add_file_to_db(self, path):
        logging.debug("Adding a new file to db: " + path)
        # TODO extract filename from path
        isDir = 1 if os.path.isdir(path) else 0
        size = os.path.getsize(path)
        cs = checksum.calc_file_checksum(path)
        self.db.add_file(path, isDir, size, cs, 0)
    
    def add_new_file(self, path):        
        # TODO
        pass

    #TODO: Probably makes more sense to make all these functions part of the peer class
    
    def _get_peer_list(self, file_path):
        peer_list_request = messages.PeerListRequest(file_path)
        communication.send_message(peer_list_request, self.tracker)
        
        peer_list_response = communication.recv_message(self.tracker)
        peer_list = peer_list_response.peer_list
        return peer_list
    
    def get_handler_method_index(self):
        return {MessageType.ARCHIVE_REQUEST : self.handle_ARCHIVE_REQUEST,
                MessageType.PEER_LIST_REQUEST : self.handle_PEER_LIST_REQUEST,
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
                }
    
    def handle_CONNECT_REQUEST(self, client_socket, msg):
        pass
    def handle_CONNECT_RESPONSE(self, client_socket, msg):
        pass
    def handle_DISCONNECT_REQUEST(self, client_socket, msg):
        pass
    def handle_DISCONNECT_RESPONSE(self, client_socket, msg):
        pass
    
    def handle_PEER_LIST_REQUEST(self, client_socket, msg):
        pass

    def handle_FILE_DOWNLOAD_REQUEST(self, client_socket, msg):
        pass
    
    def handle_FILE_DOWNLOAD_DECLINE(self, client_socket, msg):
        pass
    def handle_FILE_DATA(self, client_socket, msg):
        pass
    
    def handle_FILE_CHANGED(self, client_socket, msg):        
        path = msg.file_path
        new_data = msg.new_data
        remote_checksum = msg.new_checksum
        start_offset = msg.start_offset
        
        if not os.path.exists(path):
            return
        filesystem.write_file(path, new_data, start_offset)
        
        new_data = filesystem.read_file(path)
        new_checksum = checksum.calc_checksum(new_data)
        
        if (new_checksum != remote_checksum):
            communication.send_message(messages.FileDownloadRequest(path), client_socket)
    
    def handle_NEW_FILE_AVAILABLE(self, client_socket, msg):
        pass
    
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
        pass
    def handle_LIST(self, client_socket, msg):
        pass
    def handle_ARCHIVE_REQUEST(self, client_socket, msg):
        pass
    def handle_ARCHIVE_RESPONSE(self, client_socket, msg):
        pass
    
class AcceptorThread(threading.Thread):
    def __init__(self, peer):
        super(AcceptorThread, self).__init__()
        self._peer = peer

        self.alive = threading.Event()
        self.alive.set()
        # terminate this thread when the main thread exits
        threading.Thread.setDaemon(self, True)

    def run(self):        
        server_socket = self._peer.get_server_socket()
        while self.alive.is_set():            
            logging.debug("Waiting for a connection")
            client_socket, addr = server_socket.accept()
            logging.debug("Received a new connection. Spawning a HandlerThread")
            handler = HandlerThread(self._peer, client_socket)
            handler.start()
    
    def join(self, timeout=None):
        logging.debug("Ending thread")
        self.alive.clear()
        threading.Thread.join(self, timeout)

class HandlerThread(threading.Thread):
    def __init__(self, peer, client_socket):            
        super(HandlerThread, self).__init__()
        self._peer = peer
        self._client_socket = client_socket
    
    def run(self):
        logging.debug("Spawned a HandlerThread")
        received_msg = communication.recv_message(socket=self._client_socket)

        msg_type = received_msg.msg_type
        
        handler_method_index = self._peer.get_handler_method_index()
        
        handler_method = handler_method_index[msg_type]
        handler_method(self._client_socket, received_msg)
    
    def join(self, timeout=None):
        logging.debug("Ending thread")
        threading.Thread.join(self, timeout)

