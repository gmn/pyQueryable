
import re
import os, sys
import unittest
import json

sys.path.append(os.path.realpath('.'))
sys.path.append(os.path.realpath('..'))
from Queryable import db_object

p = lambda s: print(str(s))

def objeq( x, y ):
    unmatched = set(x.items()) ^ set(y.items())
    return len(unmatched) == 0

# ordered array equal
def oareq( x, y ):
    for s in zip(x, y):
        if not objeq( s[0], s[1] ):
            return False
    return True

def print_inserts(self):
    _Y = lambda t,o: int(str(t)+str(o))
    db = db_object()
    for T in range(10):
        db.clear()
        s = '['
        for O in range(9):
            db.insert( {'v':_Y(T,O)} )
            s = "{}{}'_id':{},'v':{}{},".format(s,'{',(O+1),_Y(T,O),'}')
        O = 9
        db.insert( {'v':_Y(T,O)} )
        s = "{}{}'_id':{},'v':{}{}".format(s,'{',(O+1),_Y(T,O),'}')
        p( '\n-----------------------------' )
        A = db.find({'v':re.compile('.*')})
        p( A )
        p( '\n-----------------------------' )
        s = '{}]'.format(s)
        p( s )
        self.assertEqual(A, eval(s))
        p( '=======================================================\n' )


class QueryableTests(unittest.TestCase):
    def test_exists(self):
        db = db_object()
        db.insert({'a':1,'b':2})
        db.insert({'b':2,'c':3})
        db.insert({'c':3,'d':4})

        self.assertEqual( db.find({'a':{'$exists':True}}),
            [{'_id': 1, 'a': 1, 'b': 2}])
        self.assertEqual( db.find({'b':{'$exists':True}}),
            [{'a': 1, '_id': 1, 'b': 2}, {'c': 3, '_id': 2, 'b': 2}])
        self.assertEqual( db.find({'c':{'$exists':True}}),
            [{'c': 3, '_id': 2, 'b': 2}, {'c': 3, 'd': 4, '_id': 3}])
        self.assertEqual( db.find({'b':{'$exists':False}}),
            [{'c': 3, 'd': 4, '_id': 3}])
        self.assertEqual( db.find({'b':{'$exists':False},'c':{'$exists':False}}), [])

    def test_insert(self):
        print_inserts(self)
        pass
    def test_remove(self):
        pass
    def test_or(self):
        pass
    def test_in(self):
        pass
    def test_ne(self):
        pass
    def test_gt_gte_lt_lte(self):
        pass
    def test_regex(self):
        pass

if __name__ == '__main__':
    unittest.main()
