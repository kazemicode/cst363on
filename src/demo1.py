#!/usr/bin/env python3
#encoding: windows-1252
from sdb import * 
from schema import * 
import random

# to create a database, specify the schema columns
# column "id" has a unique index
# TEXT type columns must have a length declared
# INTEGER, DOUBLE and TEXT are the only allowed datatypes.  
# null values are not supported.

c1 = Column("id", Column.INTEGER, index=False, unique=False)
c2 = Column("salary", Column.DOUBLE, index=False, unique=False)
c3 = Column("name", Column.TEXT, length=20,index=False, unique=False)
emp = Schema("employee", (c1, c2, c3))
db = SimpleDB(emp)
db.create()

# at this point a file exsists on disk with an empty database and 
# room for max of 4096 rows.

# generate some random data and insert the row into the database

for i in range(50):
    id = i 
    salary = random.uniform(10000,20000)
    name = 'David S. P'+str(i)
    row = Row(db.schema, (id, salary, name))
    db.insertRow(row)
db.write()

# you must do a write to save the data to the file

# print is for debugging and will list the row data in raw format

db.print(indexes=True)