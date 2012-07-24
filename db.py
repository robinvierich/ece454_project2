"""
db.py - Persistent storage for peers
"""

import sqlite3
import logging
import Queue
import threading

def wait_for_commit_queue(function):
    """A decorator that waits for the commit queue to be empty, 
        then calls the function
        """

    def wrapper(*args, **kwargs):
        db = args[0]
        logging.debug(function.func_name + " - Waiting until the DB Commit queue is empty")
        db.q.join()
        logging.debug("Commit queue is now empty. Executing query")

        return_value = function(*args, **kwargs)
        return return_value
        
    return wrapper

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

    @wait_for_commit_queue
    def list_files(self, path):
        # for now, this just lists all files that the tracker knows about
        logging.debug("Listing files")
        
        with self.connection:
            query = ("SELECT * FROM FILES")
            self.cur.execute(query)
            res = self.cur.fetchall()            
            return res

    # TODO add parents
    @wait_for_commit_queue
    def add_or_update_file(self, file_model):
        
        fileName = file_model.filename 
        is_directory = file_model.is_directory
        size = file_model.size
        checksum = file_model.checksum
        last_ver_num = file_model.last_ver_num
        
        logging.debug("Adding a new entry in Files table")
        
        with self.connection:
            query = ("""
begin tran
   update table with (serializable) set ...
   where kay = @key

   if @@rowcount = 0
   begin
          insert table (key, ...) values (@key,..)
   end
commit tran""")
            
        self.q.put((query, [fileName, str(is_directory), str(size),
                            sqlite3.Binary(checksum), str(last_ver_num)]))
        
    @wait_for_commit_queue        
    def add_file(self, file_model):      
        query = ("INSERT INTO Files " +
                 "(FileName, IsDirectory, Size, GoldenChecksum, LastVersionNumber) " +
                 "VALUES (?, ?, ?, ?, ?)")
        fileName = file_model.filename 
        is_directory = file_model.is_directory
        size = file_model.size
        checksum = file_model.checksum
        last_ver_num = file_model.last_ver_num

        self.q.put((query, [fileName, str(is_directory), str(size),
                            sqlite3.Binary(checksum), str(last_ver_num)]))

    # Delete everything from the files table and repopulate it with file_list
    @wait_for_commit_queue
    def clear_files_and_add_all(self, file_list):
        logging.debug("Adding a files into the File table")
        with self.connection:
            query = ("DELETE FROM Files")
            self.q.put((query, []))
            query = ("INSERT INTO Files VALUES (?, ?, ?, ?, ?, ?, ?)")
            self.q.put((query, file_list))


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
    @wait_for_commit_queue            
    def add_peer(self, ip, port, state, maxFileSize, maxFileSysSize, currFileSysSize, name=""):
        logging.debug("Adding a new entry in Peers table")
        
        # TODO Check if the peer with the same Port and IP is already there
        # in which case just updated its state?
        query = ("INSERT INTO Peers " +
                 "(Name, Ip, Port, State, MaxFileSize, MaxFileSysSize, CurrFileSysSize) " +
                 "VALUES (?, ?, ?, ?, ?, ?, ?)")
        
        self.q.put((query, [name, ip, port, str(state), str(maxFileSize),
                            str(maxFileSysSize), str(currFileSysSize)]), [])

    @wait_for_commit_queue
    def get_peer_state(self, ip):
        with self.connection:
            query = ("SELECT State FROM Peers " +
                 "WHERE ip='%s'" % ip)
            self.cur.execute(query)
            res = self.cur.fetchone()
            
            return res[0]
        
    @wait_for_commit_queue        
    def get_peer_list(self, file_path=None):
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
    @wait_for_commit_queue
    def clear_peers_and_insert(self, peers_list):
        query = "DELETE FROM Peers"
        self.q.put((query, []))
        query = "INSERT INTO Peers VALUES (?, ?, ?, ?, ?)"
        self.q.put((query, peers_list))
        
    @wait_for_commit_queue
    def get_peer_list(self):
        with self.connection:
            # just give them the list of all peers
            query = "SELECT Id, Name, Ip, Port, State FROM Peers"
            
            self.cur.execute(query)
            res = self.cur.fetchall()
            if res is None:
                raise RuntimeError("Cannot get a peers list (LocalPeerDb)")
            return res

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
