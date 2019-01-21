#!/usr/bin/env python3
#encoding: windows-1252
from sdb import * 

# open and print rows and indexes of existing database
# schema information:  table name and column information are printed
# rows are printed with rowid and row in raw(string) format
# indexes are printed showing value and rowid of each index entry

db = SimpleDB("employee")
db.print(indexes=True)