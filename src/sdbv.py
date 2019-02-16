from schema import *
from sdb import *
import traceback

class ChangeRecord():
    INSERT = 1
    UPDATE = 2
    DELETE = 3
    BEFORE = 4 # for part 3along with version_id to track history
    
    def __init__(self, version_id, kind, rowid, rawdata):
        self.version_id = version_id
        self.kind = kind  # INSERT, UPDATE or DELETE change record
        self.rowid = rowid
        self.change = rawdata
         

class SimpleDBV():
# SimpleDB with versioning concurrency
    def __init__(self, schema):
        self.sdb = SimpleDB(schema) 
        self.schema = self.sdb.schema
        self.row_versionid = [0] * 4096   # a version id for each row_versionid
        self.row_history = dict()       # key=rowid, value=list of committed change records
        self.transactions = dict()      # key=tranid (versionid), value = list of change records
        self.sdb.b1.unreserveAll()
        self.next_version_id = 1;
          
    def create(self):
        self.sdb.create()
          
    def write(self):
        self.sdb.write()
          
    def print(self, indexes=False):
        # for debug - print out the contents of database
        self.sdb.print(indexes)
          
    def getRow(self, rowid, version_id):
        trnlog = self.transactions[version_id]
        for i in range(len(trnlog)-1, -1, -1): #  traverse backwards
            cr = trnlog[i]
            if cr.rowid == rowid:

                return Row(self.sdb.schema, cr.change)
        # reached only if current trns id not in transactions
        if self.row_versionid[rowid] < version_id:
            return self.sdb.getRow(rowid)
        else:
            for record in self.row_history[rowid]:
                if record.version_id < version_id:
                    return Row(self.schema, record.change)

 
    def insertRow(self, row, version_id):
        rowid = self.sdb.b1.findSpace(0)
        self.sdb.b1.reserve(rowid)  # reserve the rowid until commit
        cr = ChangeRecord(version_id, ChangeRecord.INSERT, rowid, row.getRaw())
        self.transactions[version_id].append(cr)  # add the ChangeRecord object to the tr log
        return rowid
          
    def deleteRow(self, rowid, version_id):
        cr = ChangeRecord(version_id, ChangeRecord.DELETE, rowid, b'')
        self.transactions[version_id].append(cr)  # add the ChangeRecord object to the tr log
        return True
          
    def updateRow(self, rowid, new_row, version_id):
        cr = ChangeRecord(version_id, ChangeRecord.UPDATE, rowid, new_row.getRaw())
        self.transactions[version_id].append(cr)    # add the ChangeRecord object to the tr log
        return True
          
    def startTransaction(self):
        trnid = self.getNextId()
        self.transactions[trnid] = []     # put trnid and empty list of change records into tran dictionary
        return trnid
     
    def getNextId(self):
        trnid = self.next_version_id
        self.next_version_id = self.next_version_id + 1 
        return trnid          
          
    def commit(self, version_id):
        for cr in self.transactions[version_id]:
            # Only commit if we will not conflict with a later version
            if self.row_versionid[cr.rowid] < version_id:

                if cr.kind == ChangeRecord.DELETE:
                    self.self.sdb.deleteRow(cr.rowid)
                elif cr.kind == ChangeRecord.UPDATE:
                    self.sdb.updateRawRow(cr.rowid, cr.change)
                elif cr.kind == ChangeRecord.INSERT:
                    self.sdb.insertRawRowId(cr.rowid, cr.change)
                    self.sdb.b1.set(cr.rowid)

                # use commit_id and add to database's list of row versions
                # used to prevent dirty reads
                commit_id = self.getNextId()
                cr.version_id = commit_id
                self.row_versionid[cr.rowid] = commit_id
                # Add to row history
                if cr.rowid in self.row_history:
                    self.row_history[cr.rowid].insert(0, cr) # insert at front
                else:
                    self.row_history[cr.rowid]= [cr] # create an entry if one doesn't exist
            else:
                # rollback if we conflict
                self.rollback(version_id)
                return False

        return True

    def rollback(self, version_id):
        for cr in self.transactions[version_id]:
            if cr.kind == ChangeRecord.INSERT:
                self.sdb.b1.unreserve(cr.rowid)
        del self.transactions[version_id]
        return True
