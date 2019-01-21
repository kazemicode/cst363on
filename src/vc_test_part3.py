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
    

    def test_commit(self):
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
        
        
    def test_isolation_rr(self):
        trnid=self.db.startTransaction()
        rowid1 = self.db.insertRow(Row(self.db.schema, [10, "tom", "cs", 10]), trnid)
        rowid2 = self.db.insertRow(Row(self.db.schema, [20, "david", "math", 25]), trnid)
        rowid3 = self.db.insertRow(Row(self.db.schema, [30, "ana", "cs", 45]), trnid)
        self.assertTrue(self.db.commit(trnid))
        trnid2 = self.db.startTransaction()
        rowt2 = self.db.getRow(rowid1, trnid2)
        rowt2.values[3]=14
        self.db.updateRow(rowid1, rowt2, trnid2)
        trnid3 = self.db.startTransaction()
        #      trnid2       trnid3       trnid4
        #      getRow(0)    
        #      updateRow(0) start()
        #                   getRow(0) should not see update
        #      commit()
        #                   getRow(0) should not see update
        #                   commit()
        #                                 start()
        #                                 getRow(0) should see update
        #                                 commit()
        #              
        rowt3 =   self.db.getRow(rowid1, trnid3)
        self.assertEqual(rowt3.values[3], 10)
        self.assertTrue(self.db.commit(trnid2))
        rowt3 =   self.db.getRow(rowid1, trnid3)
        self.assertEqual(rowt3.values[3], 10)
        self.assertTrue(self.db.commit(trnid3))
        trnid4 = self.db.startTransaction()
        rowt4 = self.db.getRow(rowid1, trnid4)
        self.assertEqual(rowt4.values[3], 14)
        self.assertTrue(self.db.commit(trnid4))

    def test_rollback(self):
        trnid=self.db.startTransaction()
        rowid1 = self.db.insertRow(Row(self.db.schema, [10, "tom", "cs", 10]), trnid)
        rowid2 = self.db.insertRow(Row(self.db.schema, [20, "david", "math", 25]), trnid)
        rowid3 = self.db.insertRow(Row(self.db.schema, [30, "ana", "cs", 45]), trnid)
        self.assertTrue(self.db.commit(trnid))
        trnid2 = self.db.startTransaction()
        rowt2 = self.db.getRow(rowid1, trnid2)
        rowt2.values[3]=14
        self.db.updateRow(rowid1, rowt2, trnid2)
        trnid3 = self.db.startTransaction()
        #      trnid2       trnid3       trnid4
        #      getRow(0)    
        #      updateRow(0) start()
        #                   getRow(0) should not see update
        #      rollback()
        #                   getRow(0) should not see update
        #                   rollback()
        #                                 start()
        #                                 getRow(0) see original row
        #                                 rollback()
        #              
        rowt3 =   self.db.getRow(rowid1, trnid3)
        self.assertEqual(rowt3.values[3], 10)
        self.assertTrue(self.db.rollback(trnid2))
        rowt3 =   self.db.getRow(rowid1, trnid3)
        self.assertEqual(rowt3.values[3], 10)
        self.assertTrue(self.db.rollback(trnid3))
        trnid4 = self.db.startTransaction()
        rowt4 = self.db.getRow(rowid1, trnid4)
        self.assertEqual(rowt4.values[3], 10)
        self.assertTrue(self.db.rollback(trnid4))
        
    def test_commitConflict(self):
        trnid=self.db.startTransaction()
        rowid1 = self.db.insertRow(Row(self.db.schema, [10, "tom", "cs", 10]), trnid)
        rowid2 = self.db.insertRow(Row(self.db.schema, [20, "david", "math", 25]), trnid)
        rowid3 = self.db.insertRow(Row(self.db.schema, [30, "ana", "cs", 45]), trnid)
        self.assertTrue(self.db.commit(trnid))
        trnid2 = self.db.startTransaction()
        rowt2 = self.db.getRow(rowid1, trnid2)
        rowt2.values[3] = 14
        self.db.updateRow(rowid1, rowt2, trnid2)
        trnid3 = self.db.startTransaction()
        #      trnid2       trnid3       trnid4
        #      getRow(0)    
        #      updateRow(0) start()
        #                   getRow(0) should not see update
        #                   updateRow(0) change 10->16
        #                   commit()  should be ok
        #      commit()  should fail
        #                              start()
        #                              getRow(0)
        #                              verify value is 16
        rowt3 =   self.db.getRow(rowid1, trnid3)
        self.assertEqual(rowt3.values[3], 10)
        rowt3.values[3] = 16
        self.db.updateRow(rowid1, rowt3, trnid3)
        self.assertTrue(self.db.commit(trnid3))
        self.assertFalse(self.db.commit(trnid2))
        trnid4 = self.db.startTransaction()
        rowt4 = self.db.getRow(rowid1, trnid4)
        self.assertEqual(rowt4.values[3], 16)
        self.assertTrue(self.db.commit(trnid4))
        


if __name__ == '__main__':
    unittest.main()
