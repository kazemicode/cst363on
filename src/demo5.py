from sdb import * 
from sdbfilter import * 
import random

# create schema.  id has unique index.  salary has non-unique index

c1 = Column("id", Column.INTEGER, index=True, unique=True)
c2 = Column("salary", Column.DOUBLE, index=True, unique=False)
c3 = Column("name", Column.TEXT, length=20,index=False, unique=False)
emp = Schema("employee2", (c1, c2, c3))
db = SimpleDB(emp)
db.create()

# at this point a file exsists on disk with an empty database and 
# room for max of 4096 rows.

# generate some random data and insert the row into the database

for i in range(50):
    id = i 
    salary = random.uniform(10000,20000)
    if i%2==0:
        name = 'David S. P'+str(i)
    else:
        name = 'Tom C'+str(i)
    row = Row(db.schema, (id, salary, name))
    db.insertRow(row)
db.write()

db.print(indexes=True)



print("search using salary index >=14500 and <=18500")
cursor = CursorIndex(db, Predicate(), "salary", 14500, 18500)
   
while cursor.next() >= 0:
    row = cursor.getRow()
    print(row.values)
    
    # the following cursor will return rows that satisfy
# salary >= 14500 and salary <= 18500 and name >='David' and name<'Davie' 
# uses index on salary 
print()
print("search should return salary between 14500 and 18500 and name begins with 'David'")
    
cursor = CursorIndex(db, AndPredicate(Compare(db, "name", Compare.GE, "David"), Compare(db, "name", Compare.LT, "Davie")), 
                     "salary", 14500, 18500)

while cursor.next() >= 0:
    row = cursor.getRow()
    print(row.values)
    

