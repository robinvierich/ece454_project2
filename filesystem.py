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

def read_file(file_path):
    if not os.path.exists(file_path):
        return None
    
    f = open(file_path, "r")
    data = "".join(f.readlines())
    f.close()
    return data
    
        