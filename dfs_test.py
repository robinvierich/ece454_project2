from tracker import Tracker
from peer import LocalPeer
import dfs

import os.path

def test_connect(pwd="test"):
    print "Running test_connect, pwd = %s" % pwd
    successful = dfs.connect(password=pwd)
    
    assert(successful == True)
    print "connect successful"

def test_read(testfile_path = "./dfs_test.txt",
              testfile_start_offset = None,
              testfile_length = None):
    
    print "Running test_read (%s, offset=%s, length=%s)" % tuple(map(str, (testfile_path, testfile_start_offset, testfile_length)))
    
    dfs.read(testfile_path, testfile_start_offset, testfile_length)
    

def test_write(testfile_path = "./dfs_test.txt", 
               testfile_contents = "Test File Contents",
               testfile_start_offset = None):
    
    print "Running test_write (%s, \"%s\", offset=%s)" % tuple(map(str, (testfile_path, testfile_contents, testfile_start_offset)))
    
    dfs.write(testfile_path, testfile_contents, testfile_start_offset)
    
    assert (os.path.exists(testfile_path))
    print "local path exists"
    
    assert (os.path.isfile(testfile_path))
    print "local path is a file"
    

def run_tests():
    print "DFS Test"
    print "Starting Tracker"
    tracker = Tracker()
    tracker.start_accepting_connections()
    
    dfs.init_local_peer(Tracker.HOSTNAME, Tracker.PORT)
    
    test_connect()
    test_write()

if __name__ == "__main__":
    run_tests()
    
