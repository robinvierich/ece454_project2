"""
db_test.py - Test file for db.by
"""

#################################################
# THIS FILE HASN'T BEEN UPDATED TO WORK WITH LATEST CHANGES IN DB!!!
#####################

from db import TrackerDb
import os
import logging
from messages import FileModel

def run():
    failed = False
    msg = ""

    logging_level = logging.DEBUG
    logging.basicConfig(level=logging.DEBUG, 
                        format="%(threadName)s %(filename)s %(funcName)s: %(message)s")

    if os.path.exists(TrackerDb.DB_FILE):
        os.remove(TrackerDb.DB_FILE)
    trackerDb = TrackerDb()
    
    f = FileModel(path="file1.txt", 
                  is_dir=False, 
                  size=12345, 
                  checksum="2l3kn4l23kn4", 
                  latest_version=1)
    
    trackerDb.add_file(f)    

    query = "SELECT count(*) FROM Files WHERE FileName='file1.txt'"
    trackerDb.cur.execute(query)
    res = trackerDb.cur.fetchone()
    if res[0] == 0:
        failed = True
        msg += "Adding a File failed\n"

    trackerDb.add_peer("127.0.0.1", "4456", 1, 1024, 65536, 12334, "peer1")
    query = "SELECT count(*) FROM Peers WHERE ip='127.0.0.1' AND port='4456'"
    trackerDb.cur.execute(query)
    res = trackerDb.cur.fetchone()
    if res[0] == 0:
        failed = True
        msg += "Adding a Peer failed\n"



    if failed:
        print "Tests failed"
        print msg

if __name__ == "__main__":
    run()
