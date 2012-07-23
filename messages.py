'''
Message Classes for Serial Communication
'''

class MessageType(object):    
    PEER_LIST_REQUEST = 0
    PEER_LIST = 1
    
    FILE_DOWNLOAD_REQUEST = 2
    FILE_DOWNLOAD_DECLINE = 3
    FILE_DATA = 4
    
    CONNECT_REQUEST = 5
    CONNECT_RESPONSE = 6

    DISCONNECT_REQUEST = 7
    DISCONNECT_RESPONSE = 8
    
    FILE_CHANGED = 9
    NEW_FILE_AVAILABLE = 10
    
    VALIDATE_CHECKSUM_REQUEST = 11
    VALIDATE_CHECKSUM_RESPONSE = 12
    
    DELETE_REQUEST = 13
    DELETE_RESPONSE = 14
    DELETE = 15
    
    MOVE_REQUEST = 16
    MOVE_RESPONSE = 17
    MOVE = 18
    
    LIST_REQUEST = 19
    LIST = 20
    
    ARCHIVE_REQUEST = 21
    ARCHIVE_RESPONSE = 22


class Message(object):    
    def __init__(self, msg_type):
        self.msg_type = msg_type

class ConnectRequest(Message):
    def __init__(self, pwd):
        super(ConnectRequest, self).__init__(MessageType.CONNECT_REQUEST)
        self.pwd = pwd
        

class ConnectResponse(Message):
    def __init__(self, successful):
        super(ConnectResponse, self).__init__(MessageType.CONNECT_RESPONSE)
        self.successful = successful
        
    
    
class DisconnectRequest(Message):
    def __init__(self, check_for_unreplicated_files):
        super(DisconnectRequest, self).__init__(MessageType.DISCONNECT_REQUEST)
        self.check_for_unreplicated_files = check_for_unreplicated_files
    
class DisconnectResponse(Message):
    def __init__(self, should_wait):
        super(DisconnectResponse, self).__init__(MessageType.DISCONNECT_RESPONSE)
        self.should_wait = should_wait


class PeerListRequest(Message):
    def __init__(self, file_path):
        super(PeerListRequest, self).__init__(MessageType.PEER_LIST_REQUEST)
        self._filePath = file_path

class PeerList(Message):
    def __init__(self, peer_list):
        super(PeerList, self).__init__(MessageType.PEER_LIST)
        self.peer_list = peer_list

class FileDownloadRequest(Message):
    def __init__(self, file_path):
        super(FileDownloadRequest, self).__init__(MessageType.FILE_DOWNLOAD_REQUEST)
        self.file_path = file_path

class FileDownloadDecline(Message):
    def __init__(self, file_path):
        super(FileDownloadDecline, self).__init__(MessageType.FILE_DOWNLOAD_DECLINE)
        self.file_path = file_path

class FileData(Message):
    def __init__(self, file_path, file_checksum, data):
        super(FileData, self).__init__(MessageType.FILE_DATA)
        self.file_path = file_path
        self.file_checksum = file_checksum
        self.file_data = data

class FileChanged(Message):
    def __init__(self, file_path, new_checksum, new_data, start_offset):
        super(FileChanged, self).__init__(MessageType.FILE_CHANGED)
        self.file_path = file_path
        self.new_checksum = new_checksum
        self.new_data = new_data
        self.start_offset = start_offset

class ValidateChecksumRequest(Message):
    def __init__(self, file_path, file_checksum):
        super(ValidateChecksumRequest, self).__init__(MessageType.VALIDATE_CHECKSUM_REQUEST)
        self.file_path = file_path    
        self.file_checksum = file_checksum

# note, made a change to the doc we made here. Adding file_path
class ValidateChecksumResponse(Message):
    def __init__(self, file_path, valid):
        super(ValidateChecksumResponse, self).__init__(MessageType.VALIDATE_CHECKSUM_RESPONSE)
        self.file_path = file_path
        self.valid = valid


class NewFileAvailable(Message):
    def __init__(self, file_path, file_checksum, file_data):
        super(NewFileAvailable, self).__init__(MessageType.NEW_FILE_AVAILABLE)
        self.file_path = file_path    
        self.file_checksum = file_checksum
        self.file_data = file_data
    
# doc says (file_id), changed to (file_path)
class DeleteRequest(Message):
    def __init__(self, file_path):
        super(DeleteRequest, self).__init__(MessageType.DELETE_REQUEST)
        self.file_path = file_path

# added (file_path) here
class DeleteResponse(Message):
    def __init__(self, file_path, can_delete):
        super(DeleteResponse, self).__init__(MessageType.DELETE_RESPONSE)
        self.file_path = file_path 
        self.can_delete = can_delete

# doc says (file_id), changed to (file_path)
class Delete(Message):
    def __init__(self, file_path):
        super(Delete, self).__init__(MessageType.DELETE)
        self.file_path = file_path   

# doc says (file_id, dest_path) changed to (source_path, dest_path)
class MoveRequest(Message):
    def __init__(self, source_path, dest_path):
        super(MoveRequest, self).__init__(MessageType.MOVE_REQUEST)
        self.source_path = source_path
        self.dest_path = dest_path
        
# doc says just (valid) changed to (source_path, dest_path, valid)
class MoveResponse(Message):
    def __init__(self, source_path, dest_path, valid):
        super(MoveResponse, self).__init__(MessageType.MOVE_RESPONSE)
        self.source_path = source_path
        self.dest_path = dest_path
        self.valid = valid
        
# doc says (file_id, dest_path) changed to (source_path, dest_path)  
class Move(Message):
    def __init__(self, src_path, dest_path):
        super(Move, self).__init__(MessageType.MOVE)
        self.src_path = src_path
        self.dest_path = dest_path


class ListRequest(Message):
    def __init__(self, dir_path=None):
        super(ListRequest, self).__init__(MessageType.LIST_REQUEST)
        self.dir_path = dir_path
        

class List(Message):
    def __init__(self, file_list):
        super(List, self).__init__(MessageType.LIST)
        self.file_list = file_list
        
class ArchiveRequest(Message):
    def __init__(self, file_path):
        super(ArchiveRequest, self).__init__(MessageType.ARCHIVE_REQUEST)
        self.file_path = file_path

# added file_path
class ArchiveResponse(Message):
    def __init__(self, file_path, archived):
        super(ArchiveResponse, self).__init__(MessageType.ARCHIVE_RESPONSE)
        self.file_path = file_path
        self.archived = archived
        

        
