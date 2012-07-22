"""
db.py - Persistent storage for peers
"""

import sqlite3
import logging

class PeerDb():
    def __init__(self, connection):
        logging.debug("Initializing Tables Common to LocalPeer and Tracker")
        self.connection = connection
        self.cur = connection.cursor()
        self.create_common_tables()

    def create_common_tables(self):
        with self.connection:

            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='Files'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the Files table")
                self.cur.execute("CREATE TABLE Files(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                                 "FileName TEXT, IsDirectory INT, " +
                                 "Size INT, GoldenChecksum BLOB, LastVersionNumber INT, " +
                                 "ParentId INT)")

            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='Version'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the Version table")
                self.cur.execute("CREATE TABLE Version(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                                 "FileId INT, VersionNumber INT, " +
                                 "VersionName TEXT, FileSize INT, Checksum BLOB)")

            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='LocalPeerFiles'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the LocalPeerFiles table")
                self.cur.execute("CREATE TABLE LocalPeerFiles(FileId INT)")

    # TODO add parents
    def add_file(self, fileName, isDirectory, size, checksum, lastVerNum):
        logging.debug("Adding a new entry in Files table")
        with self.connection:
            query = ("INSERT INTO Files " +
                     "(FileName, IsDirectory, Size, GoldenChecksum, LastVersionNumber) " +
                     "VALUES (?, ?, ?, ?, ?)")

            self.cur.execute(query, [fileName, str(isDirectory), str(size),
                                     sqlite3.Binary(checksum), str(lastVerNum)])

class TrackerDb(PeerDb):    
    TRACKER_DB_FILE = "tracker_db.db"
    def __init__(self):
        logging.debug("Initializing Tracker Database")
        self.connect()
        self.create_tables()
        PeerDb.__init__(self, self.connection)

    def connect(self):
        logging.debug("Connecting to the database")
        self.connection = sqlite3.connect(TrackerDb.TRACKER_DB_FILE)
        self.cur = self.connection.cursor()    
    
    def create_tables(self):
        with self.connection:
            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='Peers'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the Peers table")
                self.cur.execute("CREATE TABLE Peers(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                                 "Name TEXT, Ip TEXT, Port INT, " +
                                 "State INT, MaxFileSize INT, MaxFileSysSize INT, " +
                                 "CurrFileSysSize INT)")

            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='PeerFile'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the PeerFile table")
                self.cur.execute("CREATE TABLE PeerFile(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                                 "FileId INT, PeerId INT, " +
                                 "Checksum BLOB, PendingUpdate INT)")

            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='PeerExcludedFiles'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the PeerExcludedFiles table")
                self.cur.execute("CREATE TABLE PeerExcludedFiles(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                                 "PeerId INT, FileId INT, FileNamePattern TEXT)")
                
class LocalPeerDb(PeerDb):
    LOCAL_PEER_DB_FILE = "peer_db.db"
    def __init__(self):
        logging.debug("Initializing Local Peer Database")
        self.connect()
        self.create_tables()
        PeerDb.__init__(self, self.connection)

    def connect(self):
        logging.debug("Connecting to the database")
        self.connection = sqlite3.connect(LocalPeerDb.LOCAL_PEER_DB_FILE)
        self.cur = self.connection.cursor()    
    
    def create_tables(self):
        with self.connection:
            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='Peers'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the Peers table")
                self.cur.execute("CREATE TABLE Peers(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                                 "Name TEXT, Ip TEXT, Port INT, State INT)")

            self.cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='LocalPeerExcludedFiles'")
            res = self.cur.fetchone()
            if res[0] == 0:
                logging.debug("Creating the LocalPeerExcludedFiles table")
                self.cur.execute("CREATE TABLE LocalPeerExcludedFiles(Id INTEGER PRIMARY KEY " +
                                 "AUTOINCREMENT, FileId INT, FileNamePattern TEXT)")
