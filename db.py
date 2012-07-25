"""
db.py - Persistent storage for peers
"""

import sqlite3
import logging
import Queue
import threading
import os.path
from messages import FileModel
from msilib.schema import SelfReg

def wait_for_commit_queue(function):
    """A decorator that waits for the commit queue to be empty, 
        then calls the function
        """

    def wrapper(*args, **kwargs):
        db = args[0]
        #logging.debug(function.func_name + " - Waiting until the DB Commit queue is empty")
        db.q.join()
        #logging.debug("Commit queue is now empty. Executing query")

        return_value = function(*args, **kwargs)
        return return_value
        
    return wrapper

# TODO Add Foreign Keys!!!
class PeerDb(object):
    def __init__(self, db_name):
        logging.debug("Initializing Tables Common to LocalPeer and Tracker")
        self.dblock = threading.Lock()
                
        self.db_name = db_name
        
        db_dirpath = os.path.dirname(db_name)
        
        if db_dirpath and not os.path.exists(db_dirpath):
            os.makedirs(db_dirpath)
        
        self.q = Queue.Queue()
        self.connection = None
        self.cur = None
        self.db_thread = DbThread(self)
        self.db_thread.start()
        self.create_common_tables()

    @wait_for_commit_queue
    def execute_now(self, query, params=[]):
        with self.dblock:
            self.cur.execute(query, params)
    
    @wait_for_commit_queue
    def excute_now_and_fetch_one(self, query, params=[]):
        with self.dblock:
            self.cur.execute(query, params)
            return self.cur.fetchone()
    
    @wait_for_commit_queue
    def excute_now_and_fetch_all(self, query, params=[]):
        with self.dblock:
            self.cur.execute(query, params)
            return self.cur.fetchall()

    def create_common_tables(self):
        with self.connection:

            res = self.excute_now_and_fetch_one("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='Files'")
            if res[0] == 0:
                logging.info("Creating the Files table")
                self.execute_now("CREATE TABLE Files(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "FileName TEXT, IsDirectory INT, " +
                            "GoldenChecksum BLOB, Size INT, LastVersionNumber INT, " +
                            "ParentId INT)", [])
                
        with self.connection:
            res = self.excute_now_and_fetch_one("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='Version'")
            if res[0] == 0:
                logging.info("Creating the Version table")
                self.execute_now("CREATE TABLE Version(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "FileId INT, VersionNumber INT, " +
                            "VersionName TEXT, FileSize INT, Checksum BLOB)", [])
                
        with self.connection:
            res = self.excute_now_and_fetch_one("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='LocalPeerFiles'")
            if res[0] == 0:
                logging.info("Creating the LocalPeerFiles table")
                self.execute_now("CREATE TABLE LocalPeerFiles(FileId INT)", [])

    @wait_for_commit_queue
    def list_files(self, path):
        # for now, this just lists all files that the tracker knows about
        logging.debug("Listing files")
        
        query = ("SELECT FileName, IsDirectory, GoldenChecksum, Size, LastVersionNumber "+
                 "FROM Files")
        #query = "select * from Files"
        
        res = self.excute_now_and_fetch_all(query)
        #file_model_list = [FileModel(*f) for f in res if f]
        file_model_list = []
        for f in res:
            if f is None:
                continue
            # can't pickle buffer objects which GoldenChecksums are. Need to conv to str
            file_model_list.append(FileModel(f[0], f[1], str(f[2]), f[3], f[4]))
        return file_model_list

    @wait_for_commit_queue
    def get_file(self, path):
        query = ("SELECT FileName, IsDirectory, GoldenChecksum, Size, LastVersionNumber "+ 
                 "FROM Files WHERE FileName=?")
        res = self.excute_now_and_fetch_one(query, (path,))
        if res:
            # can't pickle buffer objects which GoldenChecksums are. Need to conv to str
            return FileModel(res[0], res[1], str(res[2]), res[3], res[4])
        else:
            return None

    @wait_for_commit_queue
    def add_local_file(self, file_name):
        logging.debug("Insert or update LocalFiles")
        file_id = self.get_file_id(file_name)       
        
        if file_id is not None:
            if not self.check_file_exists_locally(file_name):
                # add a new entry
                query = "INSERT INTO LocalPeerFiles (FileId) VALUES (?)"
                self.q.put((query, [file_id]))

    @wait_for_commit_queue
    def check_file_exists_locally(self, file_name):
        logging.debug("Checking the local files for %s", file_name)
        file_id = self.get_file_id(file_name)

        if file_id is not None:
            query = "SELECT Count(*) FROM LocalPeerFiles WHERE FileId=?"
            res = self.excute_now_and_fetch_one(query, [file_id])
            if res[0] > 0:
                return True
        return False

    @wait_for_commit_queue
    def add_or_update_file(self, file_model):
        logging.debug("Insert or update on Files table")
        file_name = file_model.path 
        is_directory = file_model.is_dir
        size = file_model.size
        checksum = file_model.checksum
        last_ver_num = file_model.latest_version
        
        #with self.connection:
        # We assume no directory trees and unique file names
        res = self.get_file_id(file_name)
        if res is None:
            # add a new entry
            query = ("INSERT INTO Files " +
                     "(FileName, IsDirectory, Size, GoldenChecksum, LastVersionNumber) " +
                     "VALUES (?, ?, ?, ?, ?)")
            self.q.put((query, [file_name, is_directory, size, sqlite3.Binary(checksum), last_ver_num]))
        else:
            # Update existing one
            query = ("UPDATE Files SET FileName=?, IsDirectory=?, Size=?, GoldenChecksum=?, " +
                     "LastVersionNumber=? WHERE Id=?")
            self.q.put((query, [file_name, is_directory, size, sqlite3.Binary(checksum), last_ver_num, res]))


    @wait_for_commit_queue        
    def add_file(self, file_model):      
        query = ("INSERT INTO Files " +
                 "(FileName, IsDirectory, GoldenChecksum, Size, LastVersionNumber) " +
                 "VALUES (?, ?, ?, ?, ?)")
        
        f = file_model

        self.q.put((query, (f.path, 
                            str(f.is_dir),
                            sqlite3.Binary(f.checksum), 
                            str(f.size),
                            str(f.latest_version))
                    ))

    # Delete everything from the files table and repopulate it with file_list
    @wait_for_commit_queue
    def clear_files_and_add_all(self, file_list):
        logging.debug("Adding a files into the File table")
        query = ("DELETE FROM Files")
        self.q.put((query, []))
        query = ("INSERT INTO Files VALUES (?, ?, ?, ?, ?, ?, ?)")
        self.q.put((query, file_list))
    
    @wait_for_commit_queue
    def get_peer_id(self, peer_ip, peer_port):
        # what's the peer we're dealing with?
        query = "SELECT Id FROM Peers WHERE Ip=? AND Port=?"
        res = self.excute_now_and_fetch_one(query, [peer_ip, peer_port])
        
        if res is not None:
            return res[0]
        return None
    
    @wait_for_commit_queue
    def get_file_id(self,file_name ):
        query = "SELECT Id FROM Files WHERE fileName=?"
        res = self.excute_now_and_fetch_one(query, [file_name])
        
        if res is not None:
            return res[0]
        return None

    @wait_for_commit_queue            
    def update_peer_state(self, ip, port, state):
        logging.debug("Updating peer's state")
        res = self.get_peer_id(ip, port)
        if res is not None:
            query = ("UPDATE Peers SET state=? WHERE Id=?")
            self.q.put((query, [state, res]))

class TrackerDb(PeerDb):    
    DB_FILE = "tracker_db.db"
    def __init__(self, db_name=DB_FILE):
        logging.debug("Initializing Tracker Database")
        
        if not db_name:
            db_name = LocalPeerDb.DB_FILE
        
        PeerDb.__init__(self, db_name)
        self.create_tables()
    
    def create_tables(self):
        with self.connection:
            res = self.excute_now_and_fetch_one("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='Peers'")
            if res[0] == 0:
                logging.debug("Creating the Peers table")
                self.execute_now("CREATE TABLE Peers(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "Name TEXT, Ip TEXT, Port INT, " +
                            "State INT, MaxFileSize INT, MaxFileSysSize INT, " +
                            "CurrFileSysSize INT)", [])

            res = self.excute_now_and_fetch_one("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='PeerFile'")
            
            if res[0] == 0:
                logging.debug("Creating the PeerFile table")
                self.execute_now("CREATE TABLE PeerFile(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "FileId INT, PeerId INT, " +
                            "Checksum BLOB, PendingUpdate INT)", [])

            res = self.excute_now_and_fetch_one("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='PeerExcludedFiles'")
            if res[0] == 0:
                logging.debug("Creating the PeerExcludedFiles table")
                self.q.put(("CREATE TABLE PeerExcludedFiles(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "PeerId INT, FileId INT, FileNamePattern TEXT)", []))
    @wait_for_commit_queue            
    def add_or_update_peer(self, ip, port, state, maxFileSize, maxFileSysSize, 
                            currFileSysSize, name="", block=False):
        logging.debug("Adding a new entry in Peers table")
        res = self.get_peer_id(ip, port)
        # peer already exists. Update it, else make a new entry
        if res is not None:
            query = ("UPDATE Peers SET state=?, maxfilesize=?, maxfilesyssize=?, currfilesyssize=?," +
                     "name=? WHERE Id=?")
            if block:
                self.execute_now(query, [state, maxFileSize, maxFileSysSize, currFileSysSize, name, res])
            else:
                self.q.put((query, [state, maxFileSize, maxFileSysSize, currFileSysSize, name, res]))
        else:
            query = ("INSERT INTO Peers " +
                     "(Name, Ip, Port, State, MaxFileSize, MaxFileSysSize, CurrFileSysSize) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?)")
            
            if block:
                self.execute_now(query, [name, ip, port, state, maxFileSize,
                                         maxFileSysSize, currFileSysSize])
            else:
                self.q.put((query, [name, ip, port, state, maxFileSize,
                                    maxFileSysSize, currFileSysSize]))

    @wait_for_commit_queue
    def get_peer_state(self, ip, port):
        query = ("SELECT State FROM Peers " +
                 "WHERE Ip=? AND Port=?")
        res = self.excute_now_and_fetch_one(query, [ip, port])
        
        return res[0]
        
    @wait_for_commit_queue        
    def get_peers(self, file_path=None):
        if file_path is None:
            # just give them the list of all peers
            query = "SELECT Id, Name, Ip, Port, State FROM Peers"
        else:
            # Lookup fileID then peers ids that have this file
            res = self.get_file_id(file_path)
            if res is None:
                raise RuntimeError("Cannot find file with name " + file_path)
            
            query = "SELECT PeerId FROM PeerFile WHERE FileId=?"
            res = self.excute_now_and_fetch_all(query, [res])
            
            if not res:
                raise RuntimeError("Cannot find peer that has file " + file_path)
            # i am not proud of this line of code
            query = ("SELECT Id, Name, Ip, Port, State FROM Peers WHERE Id IN (" + 
                     str([i[0] for i in res])[1:-1] + ")")
            
        res = self.excute_now_and_fetch_all(query)

        print res
        if res is None:
            raise RuntimeError("Cannot get a peers list " + file_path)
        
        from peer import Peer
            
        peer_list = [Peer(db_peer[2], db_peer[3], db_peer[1], db_peer[4]) for db_peer in res]
            
        return peer_list            

    @wait_for_commit_queue
    def has_unreplicated_files(self, peer_ip, peer_port):
        logging.debug("Checking if a peer has unreplicated files")
        res = self.get_peer_id(peer_ip, peer_port)
        if res is None:
            raise RuntimeError("Cannot find peer!")
        # this is a bit of a nasty query to find # of unreplicated files. tested, seems to work
        query = ("SELECT count(*) FROM PeerFile WHERE FileId NOT IN " +
                 "(SELECT FileId FROM PeerFile WHERE FileId IN " +
                 "(SELECT FileId FROM PeerFile WHERE PeerId=?) AND PeerId!=?) AND PeerId=?")
        res = self.excute_now_and_fetch_one(query, [res, res, res])
        return False if res[0] == 0 else True
    
    @wait_for_commit_queue
    def add_file_peer_entry(self, file_model, peer_ip, peer_port):
        peer_id = self.get_peer_id(peer_ip, peer_port)
        if peer_id is None:
            raise RuntimeError("Cannot find peer %s:%i" % (peer_ip, peer_port))
        file_id = self.get_file_id(file_model.path)
        if file_id is None:
            raise RuntimeError("Cannot find file " + file_model.path)
        query = "SELECT Id FROM PeerFile WHERE FileId=? AND PeerId=?"
        res = self.excute_now_and_fetch_one(query, [file_id, peer_id])
        
        if res is None:
            query = ("INSERT INTO PeerFile (FileId, PeerId, Checksum, PendingUpdate) " +
                     "VALUES (?, ?, ?, ?)")
            # TODO add pending update
            self.q.put((query, [file_id, peer_id, sqlite3.Binary(file_model.checksum), 0]))
        else:
            query = ("UPDATE PeerFile SET FileId=?, PeerId=?, Checksum=?, PendingUpdate=? " +
                     "WHERE Id=?")
            # TODO add pending update
            self.q.put((query, [file_id, peer_id, sqlite3.Binary(file_model.checksum), 0, res[0]]))
    
    @wait_for_commit_queue
    def get_peers_to_replicate_file(self, file_model, peer_ip, peer_port, max_replication):
        peer_id = self.get_peer_id(peer_ip, peer_port)
        
        from peer import PeerState
        
        # not going to worry about replication level for now
        if peer_id is not None:
            #query = ("SELECT Id, Ip, Port FROM Peers " +
            #         "WHERE Id!=? AND State=? AND MaxFileSize>=? " +
            #         "AND MaxFileSysSize>=CurrFileSysSize+?")            
            query = ("SELECT Id, Ip, Port FROM Peers " +
                     "WHERE Id!=? AND State=?")
            #self.cur.execute(query, [peer_id, PeerState.ONLINE, file_model.size, 
            #                         file_model.size])
            return self.excute_now_and_fetch_all(query, [peer_id, PeerState.ONLINE])
        else:
            query = ("SELECT Id, Ip, Port FROM Peers WHERE " +
                     "State=?")
            return self.excute_now_and_fetch_all(query, [PeerState.ONLINE])
                
            #query = ("SELECT Id, Ip, Port FROM Peers WHERE " +
            #         "State=? AND MaxFileSize>=? " +
            #         "AND MaxFileSysSize>=CurrFileSysSize+?")
            
            #self.cur.execute(query, [PeerState.ONLINE, file_model.size, 
            #                         file_model.size])
        
    @wait_for_commit_queue
    def check_checksum(self, file_path, checksum):
        file_id = self.get_file_id(file_path)
        if file_id is None:
            raise RuntimeError("Cannot find file " + file_path)
        query = "SELECT Checksum FROM Files WHERE FileId=? AND Checksum=?"
        res = self.excute_now_and_fetch_one(query, [file_id, sqlite3.Binary(checksum)])
        
        return False if res is None else True

    @wait_for_commit_queue
    def peer_has_file(self, file_path, peer_ip, peer_port):
        file_id = self.get_file_id(file_path)
        peer_id = self.get_peer_id(peer_ip, peer_port)
        # TODO there is probably a better way of handling this
        if file_id is None:
            raise RuntimeError("Cannot find file " + file_path)
        if peer_id is None:
            raise RuntimeError("Cannot find peer " + peer_ip + " " + peer_port)
        query = "SELECT count(*) FROM PeerFile WHERE FileId=? AND PeerId=?"
        res = self.excute_now_and_fetch_one(query, [file_id, peer_id])
        
        return True if res[0] == 1 else False
                
class LocalPeerDb(PeerDb):
    DB_FILE = "peer_db.db"
    def __init__(self, db_name=DB_FILE):
        logging.debug("Initializing Local Peer Database")
        
        if not db_name:
            db_name = LocalPeerDb.DB_FILE
        PeerDb.__init__(self, db_name)
        self.create_tables()

    def create_tables(self):
        with self.connection:
            res = self.excute_now_and_fetch_one("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='Peers'")
            
            if res[0] == 0:
                logging.debug("Creating the Peers table")
                self.execute_now("CREATE TABLE Peers(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                                 "Name TEXT, Ip TEXT, Port INT, State INT)", [])

            res = self.excute_now_and_fetch_one("SELECT count(*) FROM sqlite_master WHERE type='table' " +
                             "AND name='LocalPeerExcludedFiles'")
            
            if res[0] == 0:
                logging.debug("Creating the LocalPeerExcludedFiles table")
                self.execute_now("CREATE TABLE LocalPeerExcludedFiles(Id INTEGER PRIMARY KEY " +
                                 "AUTOINCREMENT, FileId INT, FileNamePattern TEXT)", [])

    # delete everything in the peers table and insert
    @wait_for_commit_queue
    def clear_peers_and_insert(self, peers_list):
        query = "DELETE FROM Peers"
        self.q.put((query, []))
        
        for p in peers_list:
            query = "INSERT INTO Peers (Name, Ip, Port, State) VALUES (?, ?, ?, ?)"
            self.q.put((query, (p.name, p.hostname, p.port, p.state)))
        
    @wait_for_commit_queue
    def get_peers(self):
        # just give them the list of all peers
        query = "SELECT Id, Name, Ip, Port, State FROM Peers"
            
        res = self.excute_now_and_fetch_all(query)
        
        if not res:
            raise RuntimeError("Cannot get a peers list (LocalPeerDb)")
        return res

    @wait_for_commit_queue            
    def add_or_update_peer(self, ip, port, state, name=""):
        logging.debug("Adding a new entry in Peers table")
        res = self.get_peer_id(ip, port)
        # peer already exists. Update it, else make a new entry
        if res is not None:
            query = ("UPDATE Peers SET state=?, name=? WHERE Id=?")
            self.q.put((query, [state, name, res]))
        else:
            query = ("INSERT INTO Peers " +
                     "(Name, Ip, Port, State) " +
                     "VALUES (?, ?, ?, ?)")
            
            self.q.put((query, [name, ip, port, state]))





class DbThread(threading.Thread):
    def __init__(self, db):            
        super(DbThread, self).__init__()
        self.db = db
        self.name = "DBThread"
        logging.debug("Connecting to the database")
        
        
        
        try:
            db.connection = sqlite3.connect(db.db_name, check_same_thread=False)
        except sqlite3.OperationalError, e:
            logging.fatal("Error connecting to DB at %s: %s" % (db.db_name, e))
            import sys
            sys.exit()
        
        db.cur = db.connection.cursor()    
        self.alive = threading.Event()
        self.alive.set()
        # terminate this thread when the main thread exits
        threading.Thread.setDaemon(self, True)
    
    def run(self):
        logging.debug("Spawned a Database Thread")
        while self.alive.is_set():
            item = self.db.q.get(block=True)
            logging.debug("Performing a db statement: " + item[0] + " " + str(item[1]))
            #with self.db.connection:
            
            success = False
            while not success: 
                if isinstance(item[0][0], (list, tuple)):
                    with self.db.dblock:
                        self.db.cur.executemany(item[0], item[1])
                else:
                    with self.db.dblock:
                        self.db.cur.execute(item[0], item[1])
                self.db.connection.commit()
                self.db.q.task_done()
                success = True
                
        logging.debug("DbThread finishing run")

    def join(self, timeout=None):
        logging.debug("Ending thread")
        threading.Thread.join(self, timeout)
