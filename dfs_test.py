from tracker import Tracker
import dfs

def run():
    print "DFS Test"
        
    print "Starting Tracker"
    tracker = Tracker()   
    tracker.start_accepting_connections()
    
    pwd = "test"
    print "Running Connect, pwd = %s" % pwd
    dfs.connect(password=pwd)
    
    testfile_path = "./dfs_test.txt"
    testfile_contents = "Test File Contents"
    testfile_start_offset = None
    print "Running write (%s, \"%s\", offset=%s)" % tuple(map(str, (testfile_path, testfile_contents, testfile_start_offset)))
    
    dfs.write(testfile_path, testfile_contents, testfile_start_offset)