#!/usr/bin/env python3
#encoding: windows-1252
class Schema:
    def __init__(self, name='', cols=()):
        # Schema object contains table name, a list of columns and size of a row in bytes.
        self.name = name
        self.cols=cols
        self.rowsize = 0 
        self.calcRowSize()
        
    def toString(self):
        # return a string representation of schema information
        s = '[ Table '+self.name+' '
        for c in self.cols:
            s = s + c.toString()
        s = s+' ]'
        return s

    def fromString(self, s):
        # load schema attributes and column information from string s.
        tokens = s.split()
        self.name = tokens[2]   # table name
        self.cols=[]
        i=3
        while tokens[i]=='[':
            i = i+2
            self.cols.append(Column(tokens[i],int(tokens[i+1]),
                                    int(tokens[i+2]),
                                    (tokens[i+3]=='True'), (tokens[i+4]=='True')))
            i=i+6
        i = i +4
        self.calcRowSize()
        
    def calcRowSize(self):
        # calculate row length
        for c in self.cols:
            self.rowsize = self.rowsize + c.length
        if self.rowsize > 4096:
            print("ERROR.  Row size exceeds 4096", self.rowsize)
        

          
class Column:
    # symbolic names for the allowed Column datatypes
    INTEGER = 1
    DOUBLE = 2
    TEXT = 3
    
    def __init__(self, colname, coltype, length=0, index=False, unique=False):
        self.colname = colname
        self.coltype = coltype
        self.index = index
        self.unique = unique
        if coltype == Column.INTEGER:
            self.length = 11
        elif coltype == Column.DOUBLE:
            self.length = 25
        elif coltype == Column.TEXT:
            self.length = length
        else:
            print("Error.  invalid column type.", coltype)

    def toString(self):
        # return string representation of column information
        return ' [ Column '+self.colname+ ' '+str(self.coltype)+' '+str(self.length)+' ' + str(self.index)+' '+str(self.unique)+' ] '
