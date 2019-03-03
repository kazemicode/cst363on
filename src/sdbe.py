from schema import *
from sdb import *
import traceback


class ChangeRecord():
    INSERT = 1
    UPDATE = 2
    DELETE = 3
    BEFORE = 4  # for part 3along with version_id to track history

    def __init__(self, version_id, kind, rowid, rawdata):
        self.version_id = version_id
        self.kind = kind  # INSERT, UPDATE or DELETE change record
        self.rowid = rowid
        self.change = rawdata


class SimpleDBE():
    # SimpleDB with versioning concurrency
    def __init__(self, schema):
        self.sdb = SimpleDB(schema)
        self.schema = self.sdb.schema
        self.bitmaps = [self.b1] # will be read from file in __open__()

    def create(self):
        self.sdb.create()

    def write(self):
        self.sdb.write()

    def print(self, indexes=False):
        # for debug - print out the contents of database
        self.sdb.print(indexes)

    def getRow(self, rowid, version_id):
        trnlog = self.transactions[version_id]
        for i in range(len(trnlog) - 1, -1, -1):  # traverse backwards
            cr = trnlog[i]
            if cr.rowid == rowid:
                if cr.change == b'':
                    return False
                else:
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
        self.transactions[version_id].append(cr)  # add the ChangeRecord object to the tr log
        return True

    def startTransaction(self):
        trnid = self.getNextId()
        self.transactions[trnid] = []  # put trnid and empty list of change records into tran dictionary
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
                    self.sdb.deleteRow(cr.rowid)
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
                    self.row_history[cr.rowid].insert(0, cr)  # insert at front
                else:
                    self.row_history[cr.rowid] = [cr]  # create an entry if one doesn't exist
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


class BitMap:
    def __init__(self, barray=bytearray(4096)):
        self.array = barray
        if len(self.array) != 4096:
            print("Error.  Initializing BitMap requires array of 4096 bytes!")

    def __getitem__(self, index):
        return self.array[index]

    def __setitem__(self, index, value):
        self.array[index] = value

    def getByteArray(self):
        return self.array

    def findSpace(self, start_rowid):
        for i in range(start_rowid, 4096):
            if self.array[i] == 0:
                return i
        return -1

    def findRow(self, start_rowid):
        for i in range(start_rowid, 4096):
            if self.array[i] == 1:
                return i
        return -1

    def unreserveAll(self):
        for i in range(4096):
            if self.array[i] == 2:
                self.array[i] = 0

    def reserve(self, rowid):
        self.array[rowid] = 2

    def set(self, rowid):
        self.array[rowid] = 1

    def unreserve(self, rowid):
        self.array[rowid] = 0
