
import re
import os, sys
import unittest

sys.path.append(os.path.realpath('.'))
sys.path.append(os.path.realpath('..'))
from Queryable import db_object

def objeq( x, y ):
    unmatched = set(x.items()) ^ set(y.items())
    return len(unmatched) == 0

# ordered array equal
def oareq( x, y ):
    for s in zip(x, y):
        if not objeq( s[0], s[1] ):
            return False
    return True

p = lambda s: print(str(s))


def setup():
    global db
    db = db_object()
    db.insert({'a':1,'b':2})
    db.insert({'b':2,'c':3})
    db.insert({'c':3,'d':4})

    # exists
    p( db.find({'a':{'$exists':True}}) )
    p( db.find({'b':{'$exists':True}}) )
    p( db.find({'c':{'$exists':True}}) )

class QueryableTests(unittest.TestCase):
    def test_exists(self):
        global db
        self.assertEqual( db.find({'a':{'$exists':True}}), [{'_id':1,'a':1,'b':2}] )

    def test_exists2(self):
        global db
        self.assertTrue( oareq( db.find({'a':{'$exists':True}}),
            [{'a': 1, '_id': 1, 'b': 2}] ) )
        self.assertTrue( oareq( db.find({'b':{'$exists':True}}),
            [{'a': 1, '_id': 1, 'b': 2}, {'c': 3, '_id': 2, 'b': 2}] ) )
        self.assertTrue( oareq( db.find({'c':{'$exists':True}}),
            [{'c': 3, '_id': 2, 'b': 2}, {'c': 3, 'd': 4, '_id': 3}] ) )


if __name__ == '__main__':
    setup()
    unittest.main()
