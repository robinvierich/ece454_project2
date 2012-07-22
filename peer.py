'''
Peer.py - Remote peer model
'''

import socket
import threading
import os.path
import logging

import communication
from messages import MessageType
import messages
import checksum
import filesystem
import tracker
from db import PeerDb, LocalPeerDb, TrackerDb

class Peer(object):
    HOSTNAME = "localhost"
    PORT = 11111
    
    def __init__(self, hostname=HOSTNAME, port=PORT):
        self.hostname = hostname
        self.port = port

class LocalPeer(Peer):       
    def __init__(self, hostname=Peer.HOSTNAME, port=Peer.PORT):
        super(LocalPeer, self).__init__(hostname, port)
        
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.start_server()
        if isinstance(self, tracker.Tracker):
            self.db = TrackerDb()
            # Go through the list of peers check which are still online and announce 
            # that the tracker is now online, too
            # TODO
        else:
            self.db = LocalPeerDb()
            # Connect to the tracker
    
    def start_server(self):
        connected = False
        while not connected:
            try:
                self._server_socket.bind((self.hostname, self.port))
                self._server_socket.listen(5)
                connected = True
            except:
                self.port += 1
    
    def persist(self, (key, value)):
        
        # we'll add sql here, just a standard dict for now
        self._persisted_data[key] = value
    
    def send_message(self, msg):
        pass
    
    def start_accepting_connections(self):
        self._acceptorThread = AcceptorThread(self)
        self._acceptorThread.start()
    
    def stop(self):
        self._acceptorThread.join(1)
    
    def get_server_socket(self):
        return self._server_socket
    
    def add_new_file(self, path):        
        logging.debug("Adding a new file: " + path)
        # TODO extract filename from path

        # add file info to the database
        isDir = 1 if os.path.isdir(path) else 0
        size = os.path.getsize(path)
        cs = checksum.calc_file_checksum(path)
        self.db.add_file(path, isDir, size, cs, 0)
        
        # I don't like to have tracker code in LocalPeer... Is there a better
        # way to structure it? For now, just get it done...

        # if it is a tracker, notify all peers
        if isinstance(self, tracker.Tracker):
            #TODO
            pass

    #TODO: Probably makes more sense to make all these functions part of the peer class
    
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
        pass
    def handle_MOVE_REQUEST(self, client_socket, msg):
        pass
    def handle_MOVE_RESPONSE(self, client_socket, msg):
        pass
    def handle_MOVE(self, client_socket, msg):
        pass
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
