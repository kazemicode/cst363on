#!/usr/bin/env python3
#encoding: windows-1252
from sdb import * 
from sdbfilter import * 

# demo of using predicates  
# to search a SimpleDB database

db = SimpleDB("employee")

# Cursor class does not any indexes
# it reads all rows in the table and 
# apply predicates returning rows that
# pass the predicates

# the following cursor will return rows that 
# satisfy  salary > 14500 and salary < 18500

cursor = Cursor(db, 
                AndPredicate(Compare(db, "salary", Compare.GE, 15000),
                             Compare(db, "salary", Compare.LE, 16500)))

while cursor.next() >= 0:
    row = cursor.getRow()
    print(row.values)