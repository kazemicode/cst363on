import unittest
from sdb import BitMap

class  Bitmap_TestCase(unittest.TestCase):

    def __init__(self,x):
        self.b  = None
        super().__init__(x)      
        
    def setUp(self):
        # this method is executed before each test
        self.b=BitMap()
    
    def test_zero(self):
        self.assertEqual(self.b[0],0)
        self.assertEqual(self.b[1],0)
        self.assertEqual(self.b[347],0)
        self.assertEqual(self.b[4095],0)
        

    def test_set(self):
        self.b[0]=1
        self.b[1]=1
        self.b[4095]=1
        self.b[347]=1
        self.assertEqual(self.b[0],1)
        self.assertEqual(self.b[1],1)
        self.assertEqual(self.b[347],1)
        self.assertEqual(self.b[4095],1)
        
    def test_unset(self):
        self.b[0]=1
        self.b[1]=1
        self.b[4095]=1
        self.b[347]=1
        self.b[0]=0
        self.b[1]=0
        self.b[4095]=0
        self.b[347]=0
        self.assertEqual(self.b[0],0)
        self.assertEqual(self.b[1],0)
        self.assertEqual(self.b[347],0)
        self.assertEqual(self.b[4095],0)
        
    def test_reserve(self):
        self.b[0]=1
        self.b[1]=1
        self.b[2]=2
        self.b[347]=1
        self.assertEqual(self.b.findSpace(0), 3)
        self.assertEqual(self.b.findRow(2), 347)
        self.assertEqual(self.b[2], 2)
        self.b[2]=0
        self.assertEqual(self.b[2], 0)
   

    def test_findSpace(self):
        self.b[0]=1
        self.b[1]=1
        self.b[2]=2
        self.b[347]=1
        self.b[4095]=1
        self.assertEqual(self.b.findSpace(0), 3)
        self.assertEqual(self.b.findSpace(4095), -1)
        self.assertEqual(self.b.findSpace(347), 348)
        
    def test_findRow(self):
        self.b[0]=1
        self.b[1]=1
        self.b[2]=2
        self.b[347]=1
        self.b[4095]=1
        self.assertEqual(self.b.findRow(0), 0)
        self.assertEqual(self.b.findRow(2), 347)
        self.assertEqual(self.b.findRow(500), 4095)


if __name__ == '__main__':
    unittest.main()