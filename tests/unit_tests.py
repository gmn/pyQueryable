
import re
import os, sys
import unittest
import json

sys.path.append(os.path.realpath('.'))
sys.path.append(os.path.realpath('..'))
from Queryable import db_object

p = lambda s: print(str(s))

# object equal
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
        #p( '\n-----------------------------' )
        A = db.find({'v':re.compile('.*')}).data
        #p( A )
        #p( '\n-----------------------------' )
        s = '{}]'.format(s)
        #p( s )
        self.assertEqual(A, eval(s))
        #p( '=======================================================\n' )


class QueryableTests(unittest.TestCase):
    def test_exists(self):
        db = db_object()
        db.insert({'a':1,'b':2})
        db.insert({'b':2,'c':3})
        db.insert({'c':3,'d':4})

        self.assertEqual( db.find({'a':{'$exists':True}}).data,
            [{'_id': 1, 'a': 1, 'b': 2}])
        self.assertEqual( db.find({'b':{'$exists':True}}).data,
            [{'a': 1, '_id': 1, 'b': 2}, {'c': 3, '_id': 2, 'b': 2}])
        self.assertEqual( db.find({'c':{'$exists':True}}).data,
            [{'c': 3, '_id': 2, 'b': 2}, {'c': 3, 'd': 4, '_id': 3}])
        self.assertEqual( db.find({'b':{'$exists':False}}).data,
            [{'c': 3, 'd': 4, '_id': 3}])
        self.assertEqual( db.find({'b':{'$exists':False},'c':{'$exists':False}}).data, [])
        p('EXISTS')

    def test_insert(self):
        print_inserts(self)
        p('SORTING, INSERT')

    def test_sorting_int(self):
        db = db_object()
        for x in [3, 5, 1, 4, 2]:
            db.insert( {'a':x} )

        r = db.find({'a':re.compile('.*')})
        #print(r.toString(min=True))
        self.assertEqual(r.data,
            [{"a": 3, "_id": 1}, {"a": 5, "_id": 2}, {"a": 1, "_id": 3}, {"a": 4, "_id": 4}, {"a": 2, "_id": 5}] )
        r.sort({'a':-1})
        #print(r.toString(min=True))
        self.assertEqual(r.data,
            [{"a": 5, "_id": 2}, {"a": 4, "_id": 4}, {"a": 3, "_id": 1}, {"a": 2, "_id": 5}, {"a": 1, "_id": 3}] )
        r.sort({'a':1})
        #print(r.toString(min=True))
        self.assertEqual(r.data,
            [{"a": 1, "_id": 3}, {"a": 2, "_id": 5}, {"a": 3, "_id": 1}, {"a": 4, "_id": 4}, {"a": 5, "_id": 2}] )
        p('SORTING, INT')

    def test_sorting_str(self):
        db = db_object(auto_index='').insert([{'a':'A'},{'a':'2'},{'a':'a'},{'a':'c'}])
        res = db.find({'a':re.compile('.*')}).sort({'a':-1})
        self.assertEqual(res.data, [{'a': 'c'}, {'a': 'a'}, {'a': 'A'}, {'a': '2'}])
        p('SORTING, STR')

    def test_remove(self):
        db = db_object(auto_index='').insert([{'a':3},{'a':5},{'b':2},{'a':1}])
        res = db.remove({'a':{'$exists':True}})._data
        self.assertEqual(res, [{'b':2}] )
        db = db_object(auto_index='').insert([{'a':3},{'a':5},{'b':2},{'a':1}])
        res = db.remove({'a':{'$exists':False}})._data
        self.assertEqual(res, [{'a': 3}, {'a': 5}, {'a': 1}] )
        p('REMOVE')

    def test_or(self):
        db = db_object(auto_index='').insert([{'a':1},{'b':2},{'c':3},{'a':4}])
        r = re.compile('.*')
        self.assertEqual( db.find({'$or':[{'a':r},{'b':2}]}).data, \
                            [{'a': 1}, {'b': 2}, {'a': 4}] )
        p('OR')

    def test_and(self):
        M = {'a':1,'b':2,'c':3,'d':4}
        db = db_object(auto_index='').insert(M)
        res = db.find(M)
        self.assertEqual( res.data, [M] )
        p('AND')

    def test_in(self):
        db = db_object(auto_index='').insert([{'a':3},{'a':5},{'b':2},{'a':1,'z':'meh'}])
        res = db.find({'a':{'$in':[2,3,5]}})
        self.assertEqual( res.data, [{'a': 3}, {'a': 5}] )
        res = db.find({'$or':[{'b':{'$in':[2]}},{'a':{'$in':[1]}}]})
        self.assertEqual( res.data, [{'b': 2}, {'a': 1,'z':'meh'}] )
        p('IN')

    def test_nin(self):
        db = db_object(auto_index='').insert([{'a':3},{'a':5},{'a':2},{'a':'meh'}])
        res = db.find({'a':{'$nin':[2,3,5]}})
        self.assertEqual(res.data, [{'a':'meh'}])
        res = db.find({'a':{'$nin':[1]}})
        self.assertEqual(res.data, [{'a':3},{'a':5},{'a':2},{'a':'meh'}])
        p('NIN')
        pass

    def test_lt(self):
        db = db_object(auto_index='').insert([{'a':3},{'a':5},{'b':2},{'a':1,'z':'meh'}])
        for i in range(10):
            db.insert({'x':i})
        res = db.find({'x':{"$lt":3}})
        self.assertEqual( res.data, [{'x':0},{'x':1},{'x':2}] )
        p('LT')

    def test_lte(self):
        db = db_object(auto_index='').insert([{'a':3},{'a':5},{'b':2},{'a':1,'z':'meh'}])
        for i in range(10):
            db.insert({'x':i})
        res = db.find({'x':{"$lte":3}})
        self.assertEqual( res.data, [{'x':0},{'x':1},{'x':2},{'x':3}] )
        p('LTE')

    def test_gt(self):
        db = db_object(auto_index='').insert([{'x':1,'n':'one'},{'x':1.5,'n':'one point five'},{'x':'two','n':2}])
        res = db.find({'x':{'$gt':1}})
        self.assertEqual(res.data, [{'x':1.5,'n':'one point five'}])
        p('GT')
        pass

    def test_gte(self):
        db = db_object(auto_index='').insert([{'x':1,'n':'one'},{'x':1.5,'n':'one point five'},{'x':'two','n':2}])
        res = db.find({'x':{'$gte':1.000}})
        self.assertEqual(res.data, [{'x':1,'n':'one'},{'x':1.5,'n':'one point five'}])
        p('GTE')
        pass

    def test_ne(self):
        db = db_object(auto_index='').insert([{'x':1},{'x':1.0},{'x':1.01},{'x':'1'}])
        res = db.find({'x':{'$ne':1}})
        self.assertEqual(res.data, [{'x':1.01}, {'x':'1'}])
        p('NE')
        pass

    def test_regex(self):
        db = db_object(auto_index='').insert([{'x':'fred'},{'x':'dead'},{'x':256},{'x':'leded'}])
        res = db.find({'x':re.compile('.*ead')})
        self.assertEqual(res.data, [{'x':'dead'}])
        res = db.find({'x':re.compile('.*ed.*')})
        self.assertEqual(res.data, [{'x':'fred'},{'x':'leded'}])
        res = db.find({'x':re.compile('[0-9]+')})
        self.assertEqual(res.data, [{'x':256}])
        res = db.find({'x':re.compile('\d+$')})
        self.assertEqual(res.data, [{'x':256}])
        res = db.find({'x':re.compile('[a-z]{5}')})
        self.assertEqual(res.data, [{'x':'leded'}])
        res = db.find({'x':re.compile('^[a-z]{4}$')})
        self.assertEqual(res.data, [{'x':'fred'},{'x':'dead'}])
        p('REGEX')
        pass

    def test_update(self):
        p('XXX - UPDATE - XXX')
        pass

    def test_upsert(self):
        p('XXX - UPSERT - XXX')
        pass

    def test_distinct(self):
        p('XXX - DISTINCT - XXX')
        pass


if __name__ == '__main__':
    unittest.main()
