from tracker import Tracker
from peer import LocalPeer


import dfs
import logging

import os.path

def test_connect(pwd="12345"):
    print "Running test_connect, pwd = %s" % pwd
    successful = dfs.connect(password=pwd)
    
    assert(successful == True)
    print "connect successful"

def test_read(testfile_path = "dfs_test.txt",
              testfile_start_offset = None,
              testfile_length = None):
    
    params = map(str, (testfile_path, testfile_start_offset, testfile_length))
    print "Running test_read (%s, offset=%s, length=%s)" % tuple(params)
    
    dfs.read(testfile_path, testfile_start_offset, testfile_length)
    

def test_write(testfile_path = "dfs_test.txt", 
               testfile_contents = "Test File Contents",
               testfile_start_offset = None):
    
    params = map(str, (testfile_path, testfile_contents, testfile_start_offset))
    print "Running test_write (%s, \"%s\", offset=%s)" % tuple(params)
    
    dfs.write(testfile_path, testfile_contents, testfile_start_offset)

def test_ls(dir_path=None):
    print "testng ls()"
    
    ls_result = dfs.ls(dir_path)
    
    print "ls() result:"
    print ls_result

def run_tests():
    logging.basicConfig(level=logging.DEBUG, 
                        format="%(filename)s.%(funcName)s(): %(message)s")
    
    print "DFS Test"
    print "Starting Tracker"
    tracker = Tracker()
    
    dfs.init_local_peer(Tracker.HOSTNAME, Tracker.PORT)
    dfs.local_peer.root_path="./peer1/"
    
    peer2 = LocalPeer(port = LocalPeer.PORT + 1, root_path="./peer2/", db_name="peer2_db.db")
    
    test_write()
    test_ls()

if __name__ == "__main__":
    run_tests()
    
