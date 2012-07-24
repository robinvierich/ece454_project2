import hashlib

def calc_checksum(file_data):
    return hashlib.sha1(file_data).digest()
    
def calc_file_checksum(filePath, blocksize=65536):
    afile = file(filePath, 'r')
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hashlib.md5().update(buf)
        buf = afile.read(blocksize)
    return str(hashlib.md5().digest())
