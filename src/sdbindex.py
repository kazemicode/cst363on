#!/usr/bin/env python3
#encoding: windows-1252
class IndexEntryU:  # unique index entry
    # IndexEntry contains a value (an integer, double or string value) and a rowid 
    def __init__(self, value, rowid):
        self.value = value
        self.rowid = rowid
        
class IndexEntryNU:  # non-unique index entry
    # IndexEntry contains a value (an integer, double or string value) and a rowid 
    def __init__(self, value):
        self.value = value
        self.rowids = []
        
class Index:
    # An index consists of a list of IndexEntry objects that are in order by their value
    # To define an index specify the database object, the column name and whether the index is UNIQUE
    # after defining an index, you must call the create method to build the index entries
    def __init__(self, db, colname, colindex):
        self.db = db
        self.entries = []            # list of IndexEntry instances    
        self.colindex = colindex     # index into column list in schema
        self.colname = colname       # column name
 

    def print(self):
        # for debug - print content of index
        print("Index", self.db.schema.cols[self.colindex].colname, "Non-Unique:")
        print("number of entries", len(self.entries))
        for entry in self.entries:
            print(entry.value, entry.rowids)
        print("Index end.")

    def create(self):
        for rowid in range(4096):
            row = self.db.getRow(rowid)
            if row != False:
                value = row.values[self.colindex]
                self.insert(rowid, value)

    def delete(self, rowid, value):
        result = self.search(value)
        ids = self.entries[result].rowids

        if result != -1  and self.entries[result].value == value:
            if len(ids) <= 1:
                self.entries.remove(self.entries[result])
            else:
                for id in ids:
                    if id == rowid:
                        ids.remove(id)

 

    def insert(self, rowid, value):
        entry = IndexEntryNU(value)
        entry.rowids.append(rowid)
        result = self.search(value)


        if result == -1:
            self.entries.append(entry)


        else:
            if self.entries[result].value == value:
                self.entries[result].rowids.append(rowid)
            else:
                self.entries.insert(result, entry)




    def search(self, value):
        # return index value of first IndexEntry 
        #         where IndexEntry.value >= value
        # return -1 if value is higher than all entries in index
        start = 0
        end = len(self.entries) -1
        while True:
            if end < start:     # value is greater than all current entries
                return -1

            mid = (start + end) // 2    # index to check


            if self.entries[mid].value < value:     # shift to right
                start = mid + 1

            elif self.entries[mid].value >= value:
                if mid < 1 or self.entries[mid-1].value < value:     # found first >= entry
                    return mid
                else:                               # shift to left
                    end = mid - 1
            else:
                return mid
            
class UniqueIndex(Index):
    # this class is code unique to index that does not allow duplicate value entries
        def __init__(self, db, colname, colindex):
            super().__init__(db, colname, colindex)
        
        def print(self):
            # for debug - print content of index s
            print("Index", self.db.schema.cols[self.colindex].colname, "Unique:")
            print("number of entries", len(self.entries))
            for entry in self.entries:
                print(entry.value, entry.rowid)
            print("Index end.")
        
        def insert(self, rowid, value):
            entry = IndexEntryU(value, rowid)
            result = self.search(value)

            if result == -1:
                self.entries.append(entry)

            elif self.entries[result].value != value:
                self.entries.insert(result, entry)
            else:
                raise ValueError('Duplicate values not permitted.')

    
        def delete(self, rowid, value):
            result = self.search(value)
            if result != -1:
                self.entries.pop(result)
        
