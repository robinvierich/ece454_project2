'''
communication.py - Socket communication methods

'''

import socket
import threading
import struct
import Queue
import cPickle as pickle

# Use a little-endian, unsigned long
MSGLEN_STRUCT_FORMAT = "<L"


peer_socket_index = {}

def _create_peer_socket(peer):
    peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    peer_socket.connect((peer.hostname, peer.port))
    return peer_socket 


def send_message(msg, peer=None, socket=None):
    
    if socket == None:
        socket = peer_socket_index.get(peer)
    
        if socket == None:
            socket = _create_peer_socket(peer)
            peer_socket_index[peer] = socket
    
    serial_msg = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
    msglen_header = struct.pack(MSGLEN_STRUCT_FORMAT, len(serial_msg))
    
    try:
        socket.sendall(msglen_header + serial_msg)
    except IOError as e:
        raise RuntimeError("cannot send msg. " + str(e))

def recv_bytes(socket, byteCount):
    msg = ''
    while len(msg) < byteCount:
        data = socket.recv(byteCount - len(msg))
        if data == '':
            raise RuntimeError("socket connection broken")
        msg = msg + data
    return msg

def recv_message(peer=None, socket=None):
    if (peer == None and socket == None):
        raise Exception("Must enter a peer or socket to receive a message")    
    
    if socket == None:
        socket = peer_socket_index.get(peer)
        if socket == None:
            socket = _create_peer_socket(peer)
            peer_socket_index[peer] = socket    
      
    msg = ''
    msglen_header = recv_bytes(socket, 4)
    
    if len(msglen_header) != 4:
        raise Exception("socket closed")
    
    msglen = struct.unpack(MSGLEN_STRUCT_FORMAT, msglen_header)[0]
    
    while len(msg) < msglen:
        chunk = socket.recv(msglen-len(msg))
        if chunk == '':
            raise RuntimeError("socket connection broken")
        msg += chunk
        
    return pickle.loads(msg)