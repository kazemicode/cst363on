# vc_test.py
from sdbv import * 
import unittest

class TestSdbVC(unittest.TestCase):

    def __init__(self,x):
        super(TestSdbVC,self).__init__(x)
        self.db = None
        
        
    def setUp(self):
        # this method is executed before each test
        c1 = Column("id", Column.INTEGER)
        c2 = Column("name", Column.TEXT, 30)
        c3 = Column("major", Column.TEXT, 30)
        c4 = Column("credits", Column.INTEGER) 
        schema = Schema("student", [c1, c2, c3, c4])
        self.db = SimpleDBV(schema)
        self.db.create()
    

    def test_insert_commit(self):
        # this test does a series of insert following by a commit.
        # a second transaction verifies that the inserts have been
        # made to the actual database.
        trnid=self.db.startTransaction()
        rowid1 = self.db.insertRow(Row(self.db.schema, [10, "tom", "cs", 10]), trnid)
        rowid2 = self.db.insertRow(Row(self.db.schema, [20, "david", "math", 25]), trnid)
        rowid3 = self.db.insertRow(Row(self.db.schema, [30, "ana", "cs", 45]), trnid)
        self.assertTrue(self.db.commit(trnid))
        trnid2 = self.db.startTransaction()
        row4 = self.db.getRow(rowid1, trnid2)
        self.assertEqual(row4.values[0], 10)
        row5 = self.db.getRow(rowid2, trnid2)
        self.assertEqual(row5.values[0], 20)
        row6 = self.db.getRow(rowid3, trnid2)
        self.assertEqual(row6.values[0], 30)
        self.assertTrue(self.db.commit(trnid2))
        
    def test_update_delete(self):
        # this test updates and deletes rows.
        # then verifies that the modifications were made after commit.
        trnid=self.db.startTransaction()
        rowid1 = self.db.insertRow(Row(self.db.schema, [10, "tom", "cs", 10]), trnid)
        rowid2 = self.db.insertRow(Row(self.db.schema, [20, "david", "math", 25]), trnid)
        rowid3 = self.db.insertRow(Row(self.db.schema, [30, "ana", "cs", 45]), trnid)
        self.assertTrue(self.db.commit(trnid))
        trnid2 = self.db.startTransaction()
        row1 = self.db.getRow(rowid1, trnid2)
        row1.values[3] = 14
        self.db.updateRow(rowid1, row1, trnid2)
        self.db.deleteRow(rowid3, trnid2)
        self.assertTrue(self.db.commit(trnid2))
        # check that rowid1 has been changed and rowid3 has been deleted
        trnid3 = self.db.startTransaction()
        rowt3 = self.db.getRow(rowid1, trnid3)
        self.assertEqual(rowt3.values[3], 14)
        self.assertFalse(self.db.getRow(rowid3, trnid3))
        self.assertTrue(self.db.commit(trnid3))  
 

    def test_rollback(self):
        # this test does updates and delete and inserts followed by rollback.
        # a third transaction verifies that none of the modifications
        # were actually made to the database.
        trnid=self.db.startTransaction()
        rowid1 = self.db.insertRow(Row(self.db.schema, [10, "tom", "cs", 10]), trnid)
        rowid2 = self.db.insertRow(Row(self.db.schema, [20, "david", "math", 25]), trnid)
        rowid3 = self.db.insertRow(Row(self.db.schema, [30, "ana", "cs", 45]), trnid)
        self.assertTrue(self.db.commit(trnid))
        trnid2 = self.db.startTransaction()
        row1 = self.db.getRow(rowid1, trnid2)
        row1.values[3] = 14
        self.db.updateRow(rowid1, row1, trnid2)
        self.db.deleteRow(rowid3, trnid2)
        rowid4 = self.db.insertRow(Row(self.db.schema, [40, "juhi", "cs", 60]), trnid2)
        self.assertTrue(self.db.rollback(trnid2))
        trnid3 = self.db.startTransaction()
        rowt3 = self.db.getRow(rowid1, trnid3)
        self.assertEqual(rowt3.values[3], 10)
        rowt3 = self.db.getRow(rowid3, trnid3)
        self.assertEqual(rowt3.values[0], 30)
        self.assertFalse(self.db.getRow(rowid4, trnid3))
        self.assertEqual(self.db.sdb.b1.array[rowid4],0)
        self.assertTrue(self.db.commit(trnid3))  
        
    def test_tranlog(self):
        # this test does updates and delete and inserts 
        # a concurrent transaction verifies that the changes
        # are not in the actual databsae until after commit
        trnid=self.db.startTransaction()
        rowid1 = self.db.insertRow(Row(self.db.schema, [10, "tom", "cs", 10]), trnid)
        rowid2 = self.db.insertRow(Row(self.db.schema, [20, "david", "math", 25]), trnid)
        rowid3 = self.db.insertRow(Row(self.db.schema, [30, "ana", "cs", 45]), trnid)
        self.assertTrue(self.db.commit(trnid))
        trnid2 = self.db.startTransaction()
        row1 = self.db.getRow(rowid1, trnid2)
        row1.values[3] = 14
        self.db.updateRow(rowid1, row1, trnid2)
        self.db.deleteRow(rowid3, trnid2)
        rowid4 = self.db.insertRow(Row(self.db.schema, [40, "juhi", "cs", 60]), trnid2)
        # verify changes have not been made yet to the actual database
        trnid3 = self.db.startTransaction()
        rowt3 = self.db.getRow(rowid1, trnid3)
        self.assertEqual(rowt3.values[3], 10)
        rowt3 = self.db.getRow(rowid3, trnid3)
        self.assertEqual(rowt3.values[0], 30)
        self.assertFalse(self.db.getRow(rowid4, trnid3))
        self.assertEqual(self.db.sdb.b1.array[rowid4],2)
        self.assertTrue(self.db.rollback(trnid3))  
        self.assertTrue(self.db.commit(trnid2))
        # trnid2 has committed. Now the changes are made to actual database
        trnid4 = self.db.startTransaction()
        self.assertEqual(self.db.getRow(rowid1, trnid4).values[3], 14)
        self.assertFalse( self.db.getRow(rowid3, trnid4))
        self.assertEqual(self.db.getRow(rowid4, trnid4).values[0], 40)
        self.assertEqual(self.db.sdb.b1.array[rowid4],1)
        self.assertTrue(self.db.rollback(trnid4))  
 
        
if __name__ == '__main__':
    unittest.main()
