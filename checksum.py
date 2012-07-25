import hashlib

def calc_checksum(file_data):    
    return hashlib.md5(file_data).digest()
    
def calc_file_checksum(filePath, block_size=65536):
    f = file(filePath, 'r')
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.digest()

def md5_for_file(f, block_size=2**20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.digest()


