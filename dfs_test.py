from tracker import Tracker
from peer import LocalPeer
import dfs

import os.path

def test_connect(pwd="12345"):
    print "Running test_connect, pwd = %s" % pwd
    successful = dfs.connect(password=pwd)
    
    assert(successful == True)
    print "connect successful"

def test_read(testfile_path = "./dfs_test.txt",
              testfile_start_offset = None,
              testfile_length = None):
    
    params = map(str, (testfile_path, testfile_start_offset, testfile_length))
    print "Running test_read (%s, offset=%s, length=%s)" % tuple(params)
    
    dfs.read(testfile_path, testfile_start_offset, testfile_length)
    

def test_write(testfile_path = "./dfs_test.txt", 
               testfile_contents = "Test File Contents",
               testfile_start_offset = None):
    
    params = map(str, (testfile_path, testfile_contents, testfile_start_offset))
    print "Running test_write (%s, \"%s\", offset=%s)" % tuple(params)
    
    dfs.write(testfile_path, testfile_contents, testfile_start_offset)
    
    assert (os.path.exists(testfile_path))
    print "local path exists"
    
    assert (os.path.isfile(testfile_path))
    print "local path is a file"
    

def run_tests():
    print "DFS Test"
    print "Starting Tracker"
    tracker = Tracker()
    
    dfs.init_local_peer(Tracker.HOSTNAME, Tracker.PORT)
    
    peer2 = LocalPeer(port = LocalPeer.PORT + 1)
    
    #test_connect()
    
    test_write()

if __name__ == "__main__":
    run_tests()
    
