from sdbindex import *
import sys

index = Index(None, "colname", 0)
index.entries = []

# insert entries with values 102..300 by 2

for i in range(100):
    index.insert(i, 300 - 2 * i)

passtest = True
# verify that entries are in order
for i in range(len(index.entries) - 1):
    if not (index.entries[i].value <= index.entries[i + 1].value):
        print("Error1.  index entries are out of order", index.entries[i].value, index.entries[i + 1].value)
        passtest = False

if not passtest:
    print("Index Test 1 did not passtest.")

i = index.search(300)
if index.entries[i].value != 300:
    passtest = False
    print("Error2.  Search did not return correct entry.")

i = index.search(102)
if index.entries[i].value != 102:
    passtest = False
    print("Error3.  Search did not return correct entry.")

i = index.search(151)
if index.entries[i].value != 152:
    passtest = False
    print("Error4.  Search did not return correct entry.")

i = index.search(100)
if index.entries[i].value != 102:
    passtest = False
    print("Error4.  Search did not return correct entry.")

i = index.search(302)
if i != -1:
    passtest = False
    print("Error5.  Search did not return correct entry.")

# duplicate entry test
for i in range(50):
    index.insert(200 + i, 151)

# verify entries are in order
for i in range(len(index.entries) - 1):
    if not (index.entries[i].value <= index.entries[i + 1].value):
        print("Error8.  index entries are out of order", index.entries[i].value, index.entries[i + 1].value)
        passtest = False

i = index.search(151)
if not (index.entries[i - 1].value < 151 and index.entries[i].value == 151):
    passtest = False
    print("Error6.  Search of duplicate entries did not return correct entry.")

# delete test
count = 0
for entry in index.entries:
    count = count + len(entry.rowids)
for i in range(count):
    m = len(index.entries) // 2
    index.delete(index.entries[m].rowids[0], index.entries[m].value)
print("after delete test")

if len(index.entries) != 0:
    passtest = False
    print("Error7.  Delete test failed.")

# test for unique index
index = UniqueIndex(None, "colname", 0)
index.entries = []

# insert entries with values 102..300 by 2

for i in range(100):
    index.insert(i, 300 - 2 * i)

# verify that entries are in order
for i in range(len(index.entries) - 1):
    if not (index.entries[i].value <= index.entries[i + 1].value):
        print("Error10.  index entries are out of order", index.entries[i].value, index.entries[i + 1].value)
        passtest = False

i = index.search(300)
if index.entries[i].value != 300:
    passtest = False
    print("Error20.  Search did not return correct entry.")

i = index.search(102)
if index.entries[i].value != 102:
    passtest = False
    print("Error30.  Search did not return correct entry.")

i = index.search(151)
if index.entries[i].value != 152:
    passtest = False
    print("Error40.  Search did not return correct entry.")

i = index.search(100)
if index.entries[i].value != 102:
    passtest = False
    print("Error40.  Search did not return correct entry.")

i = index.search(302)
if i != -1:
    passtest = False
    print("Error50.  Search did not return correct entry.")

# duplicate entry test
try:
    index.insert(200, 151)
    index.insert(201, 151)
    passtest = False
    print("Error60.  Unique index did not raise exception on duplicate values")
except:
    # print(sys.exc_info()[1])  # print exception message
    pass

# verify entries are in order
for i in range(len(index.entries) - 1):
    if not (index.entries[i].value <= index.entries[i + 1].value):
        print("Error80.  index entries are out of order", index.entries[i].value, index.entries[i + 1].value)
        passtest = False

# delete test
count = len(index.entries)
for i in range(count):
    m = len(index.entries) // 2
    index.delete(index.entries[m].rowid, index.entries[m].value)

if len(index.entries) != 0:
    passtest = False
    print("Error70.  Delete test failed.")

if passtest:
    print("All index tests passed.  Good job.")
else:
    print("One or more index tests failed.")
