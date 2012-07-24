"""
db.py - Persistent storage for peers
"""

import sqlite3
import logging
import Queue
import threading

class PeerDb(object):
    def __init__(self, db_name):
        logging.debug("Initializing Tables Common to LocalPeer and Tracker")
        self.db_name = db_name
        self.q = Queue.Queue()
        self.connection = None
        self.cur = None
        self.db_thread = DbThread(self)
        self.db_thread.start()
        self.create_common_tables()

    def create_common_tables(self):
        with self.connection:

            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='Files'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the Files table")
                self.q.put(("CREATE TABLE Files(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "FileName TEXT, IsDirectory INT, " +
                            "Size INT, GoldenChecksum BLOB, LastVersionNumber INT, " +
                            "ParentId INT)", []))

            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='Version'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the Version table")
                self.q.put(("CREATE TABLE Version(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "FileId INT, VersionNumber INT, " +
                            "VersionName TEXT, FileSize INT, Checksum BLOB)", []))

            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='LocalPeerFiles'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the LocalPeerFiles table")
                self.q.put(("CREATE TABLE LocalPeerFiles(FileId INT)", []))

    # TODO add parents
    def add_file(self, fileName, isDirectory, size, checksum, lastVerNum):
        logging.debug("Adding a new entry in Files table")
        logging.debug("Waiting until the DB Commit queue is empty")
        self.q.join()
        logging.debug("The DB Commit queue is now empty. Gonna execute the query now.")
        with self.connection:
            query = ("INSERT INTO Files " +
                     "(FileName, IsDirectory, Size, GoldenChecksum, LastVersionNumber) " +
                     "VALUES (?, ?, ?, ?, ?)")

            self.q.put((query, [fileName, str(isDirectory), str(size),
                                sqlite3.Binary(checksum), str(lastVerNum)]))

class TrackerDb(PeerDb):    
    DB_FILE = "tracker_db.db"
    def __init__(self):
        logging.debug("Initializing Tracker Database")
        PeerDb.__init__(self, TrackerDb.DB_FILE)
        self.create_tables()
    
    def create_tables(self):
        with self.connection:
            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='Peers'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the Peers table")
                self.q.put(("CREATE TABLE Peers(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "Name TEXT, Ip TEXT, Port INT, " +
                            "State INT, MaxFileSize INT, MaxFileSysSize INT, " +
                            "CurrFileSysSize INT)", []))

            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='PeerFile'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the PeerFile table")
                self.q.put(("CREATE TABLE PeerFile(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "FileId INT, PeerId INT, " +
                            "Checksum BLOB, PendingUpdate INT)", []))

            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='PeerExcludedFiles'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the PeerExcludedFiles table")
                self.q.put(("CREATE TABLE PeerExcludedFiles(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "PeerId INT, FileId INT, FileNamePattern TEXT)", []))
                
    def add_peer(self, ip, port, state, maxFileSize, maxFileSysSize, currFileSysSize, name=""):
        logging.debug("Adding a new entry in Peers table")
        logging.debug("Waiting until the DB Commit queue is empty")
        self.q.join()
        logging.debug("The DB Commit queue is now empty. Gonna execute the query now.")
        # TODO Check if the peer with the same Port and IP is already there
        # in which case just updated its state?
        query = ("INSERT INTO Peers " +
                 "(Name, Ip, Port, State, MaxFileSize, MaxFileSysSize, CurrFileSysSize) " +
                 "VALUES (?, ?, ?, ?, ?, ?, ?)")
        
        self.q.put((query, [name, ip, port, str(state), str(maxFileSize),
                            str(maxFileSysSize), str(currFileSysSize)]), [])

    def get_peer_state(self, ip):
        logging.debug("Waiting until the DB Commit queue is empty")
        self.q.join()
        logging.debug("The DB Commit queue is now empty. Gonna execute the query now.")
        with self.connection:
            query = ("SELECT State FROM Peers " +
                 "WHERE ip='%s'" % ip)
            self.cur.execute(query)
            res = self.cur.fetchone()
            
            return res[0]
            
    def get_peer_list(self, file_path=None):
        logging.debug("Waiting until the DB Commit queue is empty")
        self.q.join()
        logging.debug("The DB Commit queue is now empty. Gonna execute the query now.")
        with self.connection:
            if file_path is None:
                # just give them the list of all peers
                query = "SELECT Id, Name, Ip, Port, State FROM Peers"
            else:
                # Lookup fileID then peers ids that have this file
                query = "SELECT Id FROM Files WHERE FileName='" + file_path + "'"
                self.cur.execute(query)
                res = self.cur.fetchone()
                if res is None:
                    raise RuntimeError("Cannot find file with name " + file_path)
                query = "SELECT PeerId FROM PeerFile WHERE FileId=" + str(res[0])
                self.cur.execute(query)
                res = self.cur.fetchall()
                if res is None:
                    raise RuntimeError("Cannot find peer that has file " + file_path)
                query = "SELECT Id, Name, Ip, Port, State FROM Peers WHERE Id=" + str(res[0])

            self.cur.execute(query)
            res = self.cur.fetchall()
            if res is None:
                raise RuntimeError("Cannot get a peers list " + file_path)
            return res            
                
class LocalPeerDb(PeerDb):
    DB_FILE = "peer_db.db"
    def __init__(self):
        logging.debug("Initializing Local Peer Database")
        PeerDb.__init__(self, LocalPeerDb.DB_FILE)
        self.create_tables()

    def create_tables(self):
        with self.connection:
            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='Peers'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the Peers table")
                self.q.put(("CREATE TABLE Peers(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "Name TEXT, Ip TEXT, Port INT, State INT)", []))

            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='LocalPeerExcludedFiles'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the LocalPeerExcludedFiles table")
                self.q.put(("CREATE TABLE LocalPeerExcludedFiles(Id INTEGER PRIMARY KEY " +
                            "AUTOINCREMENT, FileId INT, FileNamePattern TEXT)", []))

    # delete everything in the peers table and insert
    def clear_peers_and_insert(self, peers_list):
        logging.debug("Waiting until the DB Commit queue is empty")
        self.q.join()
        logging.debug("The DB Commit queue is now empty. Gonna execute the query now.")
        query = "DELETE FROM Peers"
        self.q.put((query, []))
        query = "INSERT INTO Peers VALUES (?, ?, ?, ?, ?)"
        self.q.put((query, peers_list))
    
    def get_peer_list(self):
        pass

class DbThread(threading.Thread):
    def __init__(self, db):            
        super(DbThread, self).__init__()
        self.db = db
        logging.debug("Connecting to the database")
        db.connection = sqlite3.connect(db.db_name, check_same_thread=False)
        db.cur = db.connection.cursor()    
        self.alive = threading.Event()
        self.alive.set()
        # terminate this thread when the main thread exits
        threading.Thread.setDaemon(self, True)
    
    def run(self):
        logging.debug("Spwaned a Database Thread")
        while self.alive.is_set():
            item = self.db.q.get(block=True)            
            with self.db.connection:
                logging.debug("Performing a db statement: " + item[0] + " " + str(item[1]))
                try:
                   tmp = item[1][0][0]
                   self.db.cur.executemany(item[0], item[1])
                except:
                    self.db.cur.execute(item[0], item[1])
                self.db.connection.commit()
                self.db.q.task_done()
        logging.debug("DbThread finising run")

    def join(self, timeout=None):
        logging.debug("Ending thread")
        threading.Thread.join(self, timeout)
