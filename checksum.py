import hashlib

def calc_checksum(file_data):
    return hashlib.sha1(file_data)
    
