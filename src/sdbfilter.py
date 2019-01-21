#!/usr/bin/env python3
#encoding: windows-1252
from sdbindex import *
class Predicate:
    # this is based class for search predicates
    def eval(self, row):
        return True
         
class AndPredicate(Predicate):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def eval(self, row):
        return self.left.eval(row) and self.right.eval(row)
         
class OrPredicate(Predicate):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def eval(self, row):
        return self.left.eval(row) or self.right.eval(row)
        
class NotPredicate(Predicate): 
    def __init__(self, right):
        self.right = right

    def eval(self,row):
        return not self.right.eval(row)
        
class Compare(Predicate):

    EQ = 1
    NEQ = 2
    GE = 3
    GT = 4
    LE = 5
    LT = 6 
    
    def __init__(self, db, colname, op, value):
        self.colname = colname
        self.colindex = 0;
        for col in db.schema.cols:
           if colname == col.colname:
               break
           else:
               self.colindex=self.colindex+1
        if self.colindex >= len(db.schema.cols):
            raise Exception("Predicate column name invalid."+colname)
    
        self.op = op
        self.value = value
        self.oper = { Compare.EQ: self.__eq__ , 
                      Compare.NEQ: self.__neq__, 
                      Compare.GE: self.__ge__, 
                      Compare.GT: self.__gt__, 
                      Compare.LE: self.__le__,  
                      Compare.LT: self.__lt__ } 
    
    def eval(self, row):
        return self.oper[self.op](row)
       
    def __eq__(self,row):
        return row.values[self.colindex]==self.value    
        
    def __neq__(self,row):
        return row.values[self.colindex]!=self.value
        
    def __ge__(self,row):
        return row.values[self.colindex] >= self.value
        
    def __gt__(self,row):
        return row.values[self.colindex] > self.value
        
    def __le__(self,row):
        return row.values[self.colindex] <= self.value
      
    def __lt__(self,row):
        return row.values[self.colindex] < self.value   

class Cursor:
    # Cursor is object which will read all rows in db and return
    # rows that satisfy the given predicate
    def __init__(self, db, predicate):
        self.db = db
        self.predicate = predicate
        self.rowid = -1

    def getRow(self):
        # return False is there is no row to return
        # otherwise return the Row object
        if self.rowid < 0:
            return False
        else:
            return self.db.getRow(self.rowid)

    def next(self):
        # advance cursor to next row that satisfies prediate
        # return rowid or False
       self.rowid = self.db.b1.findRow(self.rowid + 1)
       while self.rowid >= 0:
          if self.predicate.eval(self.getRow()):
              return self.rowid
          self.rowid = self.db.b1.findRow(self.rowid+1)
       self.rowid = -1 
       return self.rowid

class CursorIndex:
    # use this Cursor class to use an index with range values and additional predicates
    # is does the search  select * from table where index_column_name >= start_value and index_column_name <= end_value 
    #                     and predicates
    
    def __init__(self, db, predicate, index_column_name, start_value, end_value):
        self.db = db
        self.predicate = predicate
        self.rowid = -1
        self.index = self.findIndex(index_column_name)
        self.start_value = start_value
        self.end_value = end_value
        self.index_position = -1
        self.index_rowlistposition = -1
        
    def findIndex(self, index_column_name):
        for index in self.db.indexes:
            if index.colname == index_column_name:
                return index
        print("Error.  column name is invalid", index_column_name)
        return None

    def getRow(self):
        # return False is there is no row to return
        # otherwise return the Row object
        if self.rowid < 0:
            return False
        else:
            return self.db.getRow(self.rowid)
            
            
    def next(self):
        if self.index_position == -1:
            self.index_position = self.index.search(self.start_value)
            self.index_rowlist_position = 0;
            
        while self.index_position >= 0 and self.index_position < len(self.index.entries) and self.index.entries[self.index_position].value <= self.end_value:
            if isinstance(self.index, UniqueIndex):
                # unique index
                self.rowid =  self.index.entries[self.index_position].rowid
                self.index_position = self.index_position + 1
                if self.predicate.eval(self.db.getRow(self.rowid)):
                    return self.rowid
            else:
                # non-unique index 
                while self.index_rowlistposition < len(self.index.entries[self.index_position].rowids):
                    self.rowid =  self.index.entries[self.index_position].rowids[self.index_rowlistposition]
                    self.index_rowlistposition= self.index_rowlistposition + 1
                    if self.predicate.eval(self.db.getRow(self.rowid)):
                       return self.rowid
                self.index_rowlistposition=0
                self.index_position = self.index_position + 1
        self.rowid = -1
        return self.rowid
 
