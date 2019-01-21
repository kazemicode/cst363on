#!/usr/bin/env python3
#encoding: windows-1252
from sdb import * 

# open an existing database 

db = SimpleDB("employee")

# you can retrieve data, update or delete data 
# based on rowid

row = db.getRow(7);
print("row before udpate")
row.print()
row.values[2]='New Name'
db.updateRow(7,row)
row = db.getRow(7)
print("row after update")
print(row)

# getRow(rowid) will either return the Row
# or return False if rowid does nto exist 

row = db.getRow(6);
print("row before delete")
row.print()
db.deleteRow(6)
rc = db.getRow(6)
if rc == False:
   print("Row was successfully deleted.")
else:
   print("Error.  Row not deleted")  

db.write()

# write the updated database back to file
