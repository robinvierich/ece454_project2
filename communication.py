'''
communication.py - Socket communication methods

'''

import socket
import threading
import struct
import Queue
import cPickle as pickle
import logging

# Use a little-endian, unsigned long
MSGLEN_STRUCT_FORMAT = "<L"


peer_socket_index = {}

def connect_to_peer(peer):
    '''
    returns a socket
    '''
    peer_socket = socket.create_connection((peer.hostname, peer.port))# tries ipv4 then ipv6 (TCP/IP)
    return peer_socket


def send_message(msg, topeer=None, socket=None):
    logging.debug("Sending " + str(msg))
    if socket == None:
        socket = connect_to_peer(topeer)
        peer_socket_index[topeer] = socket
    
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
        if data == '' and len(msg) < byteCount:
            logging.error("socket connection broken")
            raise RuntimeError("socket connection broken. Received %d out of %d bytes",
                               len(msg), byteCount)
        msg = msg + data
    return msg

def recv_message(frompeer=None, socket=None):    
    if (frompeer == None and socket == None):
        raise Exception("Must enter a peer or socket to receive a message")    
    
    if socket == None:
        socket = peer_socket_index.get(frompeer)
        if socket == None:
            socket = connect_to_peer(frompeer)
            peer_socket_index[frompeer] = socket
      
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
    
    new_msg = pickle.loads(msg)
    logging.debug("Received " + str(new_msg))
    return new_msg
