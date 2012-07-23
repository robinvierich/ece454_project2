"""
db_test.py - Test file for db.by
"""

from db import TrackerDb, LocalPeerDb
import os
import logging

def run():
    failed = False
    msg = ""

    logging_level = logging.DEBUG
    logging.basicConfig(level=logging.DEBUG, 
                        format="%(threadName)s %(filename)s %(funcName)s: %(message)s")

    os.remove(TrackerDb.TRACKER_DB_FILE)
    trackerDb = TrackerDb()

    trackerDb.add_file("file1.txt", 1, 12345, "2l3kn4l23kn4", 1)    
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
