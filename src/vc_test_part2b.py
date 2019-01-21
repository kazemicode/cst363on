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
    

    def test_getRow(self):
        trnid=self.db.startTransaction()
        rowid1 = self.db.insertRow(Row(self.db.schema, [10, "tom", "cs", 10]), trnid)
        rowid2 = self.db.insertRow(Row(self.db.schema, [20, "david", "math", 25]), trnid)
        rowid3 = self.db.insertRow(Row(self.db.schema, [30, "ana", "cs", 45]), trnid)
        self.assertTrue(self.db.commit(trnid))
        trnid2 = self.db.startTransaction()
        row1 = self.db.getRow(rowid1, trnid2)
        row1.values[3]=14
        self.db.updateRow(rowid1, row1, trnid2)
        self.db.deleteRow(rowid3, trnid2)
        rowid4 = self.db.insertRow(Row(self.db.schema, [40, "juhi", "cs", 60]), trnid2)
        # now verify that getRow is returning the modified data
        # for the transaction from the transaction log
        self.assertEqual(self.db.getRow(rowid1, trnid2).values[3], 14)
        self.assertFalse(self.db.getRow(rowid3, trnid2))
        self.assertEqual(self.db.getRow(rowid4, trnid2).values[0], 40)
 

if __name__ == '__main__':
    unittest.main()
