from threading import Lock
import os

lock = Lock()

def write_file(file_path, file_data, start_offset=None):
    with lock:
        file_dir = os.path.dirname(file_path)
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        
        if start_offset == None:
            start_offset = 0
        
        f = open(file_path, "w")
        f.seek(start_offset)
        f.write(str(file_data))

def read_file(file_path, start_offset=None, length=-1):    
    if not os.path.exists(file_path):
        return None
    
    f = open(file_path, "r")
    if start_offset:
        f.seek(start_offset)
    
    data = f.read(length)
    f.close()
    return data
    

def delete_file(file_path):
    
    if not os.path.exists(file_path):
        return
    
    os.remove(file_path)

def move(src_path, dest_path):
    if not os.path.exists(src_path):
        return
    
    os.renames(src_path, dest_path)

def get_local_path(peer, file_path, version=None):
    v = version
    if v is None:
        f = peer.db.get_file(file_path)
        if f:
            v = f.latest_version
        else:
            v = 1
        
    return os.path.join(peer.root_path, file_path) + "." + str(v)