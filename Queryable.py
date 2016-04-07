"""
queryable.py

OVERVIEW

Queryable.py is a super simplistic database system. It is
essentially a wrapper around JSON which gives a standardized
"JSON array full of objects" a query language through which
CRUD operations can be performed on it, as well as structured
querying. The syntax chosen for this is a simplified version
of MongoDB.

Queryable.py is designed to be as simple as possible, adding no
features beyond the basic I/O manipulation of the JSON file,
and for everything to be included in one file with the minimum
possible number of manipulations to get your data into and out
of the JSON array.

FLOW
from queryable import queryable
db = queryable.open() # returns a db_object; db_object contains the whole db in memory
res = db.find() # returns a db_result; db_result contains the rows matching find() parms
                # and the methods to process it further
"""

import json
import re

class db_result:

    def __init__(self, init_data=False):
        if init_data is False:
            self.data = []
        elif type(init_data) is type([]):
            self.data = init_data
        elif type(init_data) is type({}):
            self.data = [ init_data ]
        else:
            raise Exception('db_result: bad input')

    def push(self, obj):
        if type(obj) is type({}):
            self.data.append(obj)
        elif type(obj) is type([]):
            self.data.extend(obj)
        else:
            raise Exception('db_result: push: bad input')

    def sort(self, obj ):
        pass

    def limit(self, ival ):
        pass

    def skip(self, ival ):
        pass

    def count(self):
        return len(self.data)


# returned by queryable.open()
class db_object:
    """
    db_object requires a path to save
    """
    def __init__(self,compact=True):
        self._id = 0
        self._path = ''
        self._data = []
        self._jsonarg = {} if compact else {"indent":2}

    def path(self, _path):
        self._path = _path
        return self

    def data(self, _data):
        if type(_data) is type(''):
            self._data = json.loads(_data)
        elif type(_data) is type([]):
            self._data = _data
        else:
            raise Exception('db_object: bad input')
        return self

    def load(self, _path=False):
        if _path:
            self._path = _path
        with open(self._path, 'r') as f:
            self._data = json.load(f)
        return self

    def save(self, _path=False):
        if _path:
            self._path = _path
        with open(self._path, 'w') as f:
            f.write( json.dumps(self._data, **self._jsonarg) )
        return self

    def insert(self, row_or_ary):
        def insert_one(self, row):
            assert type(row) is type({})
            if '_id' not in row:
                row['_id'] = self.new_index()
            self._data.append(row)

        if type(row_or_ary) is type([]):
            for row in row_or_ary:
                insert_one(self, row)
        elif type(row_or_ary) is type({}):
            insert_one(self, row_or_ary)
        else:
            raise Exception('db_object: insert: bad type')
        return self

    def new_index(self):
        self._id = self._id + 1
        return self._id

    def print(self):
        print( json.dumps(self._data, indent=2) )


    @staticmethod
    def detect_clause_type(key, val):
        if type(val) is type(True) or \
                type(val) is type(1) or \
                type(val) is type(1.0) or \
                type(val) is type(None) or \
                type(val) is type(re.compile('')):
            return 'NORMAL'
        elif type(val) is type(''):
            return 'SUBDOCUMENT' if '.' in val else 'NORMAL'
        elif type(val) is type({}):
            k = list(val.keys())[0]
            if k in ['$gt','$gte','$lt','$lte','$exists','$ne','$in']:
                return 'CONDITIONAL'
            return 'SUBDOCUMENT'
        elif type(val) is type([]):
            if key == '$or':
                return 'OR'
            return 'ARRAY'
        else:
            return 'UNKNOWN'

    @classmethod
    def match_rows_NORMAL(cls, rows, test):
        assert type(rows) is type([])
        assert type(test) is type({})
        res = []

        for row in rows:
            # for each unique key in the row
            for key in row.keys():
                next_row = False
                # matches our query key and value
                if key == test['key']:
                    # regex: equiv to SQL "like" statement
                    if type(test['val']) is type(re.compile('')):
                        if re.match(test['val'], str(row[key])):
                            res.append( row )
                            next_row = True
                    # compare number, date, string statements directly
                    elif row[key] == test['val']:
                        res.append( row )
                        next_row = True
                if next_row:
                    break
        return res

    @classmethod
    def match_rows_CONDITIONAL(cls, rows, test):
        assert type(rows) is type([])
        assert type(test) is type({})
        res = []

        cond = list(test['val'].keys())[0]

        for row in rows:
            """ {key:{$exists:true/false}}
            test.key: key
            test.val: {$exists:True}
            """
            if cond == '$exists':
                if test['val'][cond] and row.get(test['key']):
                    res.append(row)
                elif not test['val'][cond] and not row.get(test['key']):
                    res.append(row)
                continue

            """
            test.key: key
            test.val: {$ne:val}
            """
            # FIXME: is this even necessary? Couldnt I just use the comparator $ne ?
            # add rows that don't contain test.key
            if cond == '$ne':
                # see if row contains test.key at all, if not add it
                if not row.get(test['key']):
                    res.append(row)
                    continue
                # else do -ne test against row's matching key
                elif row.get(test['key']) != test['val']['$ne']:
                    res.append(row)
                    continue

            # for every unique key in row
            for key in row.keys():
                # key matches
                if key == test['key']:
                    comparators = { '$lt': lambda a, b: a and b and a < b,
                                    '$lte':lambda a, b: a and b and a <= b,
                                    '$gt': lambda a, b: a and b and a > b,
                                    '$gte':lambda a, b: a and b and a >= b,
                                    '$ne': lambda a, b: a and b and a != b,
                                    '$eq': lambda a, b: a and b and a == b,
                                    '$in': lambda a, b: a and b and a in b}

                    compare = comparators.get(cond)
                    if compare is not None:
                        if compare(row[key], test['val'].get(cond)):
                            res.append(row)
                            break

        # remove the key:val from test object
        # FIXME: why do this? it should be obvious
        if cond:
            del test['val'][cond]
        return res

    @classmethod
    def match_rows_OR(cls, rows, array):
        assert type(rows) is type([])
        assert type(array) is type([])
        res = []

        for row in rows:
            for elt in array:
                eltkey = list(elt.keys())[0]
                eltval = elt[eltkey]
                test = { "key": eltkey, "val": eltval }

                _t = cls.detect_clause_type( eltkey, eltval )
                if _t == 'NORMAL':
                    if type(test['val']) is type(re.compile('')):
                        if row[test['key']] and row[test['key']].match(test['val']):
                            res.append(row)
                    elif row[test['key']] == test['val']:
                        res.append( row )
                    break
                elif _t == "CONDITIONAL":
                    def nn(a):
                        return type(a) is not type(None) and type(a) is not type(False)
                    comparators = { '$lt': lambda a, b: nn(a) and nn(b) and a < b,
                                    '$lte':lambda a, b: nn(a) and nn(b) and a <= b,
                                    '$gt': lambda a, b: nn(a) and nn(b) and a > b,
                                    '$gte':lambda a, b: nn(a) and nn(b) and a >= b,
                                    '$ne': lambda a, b: a != b,
                                    '$eq': lambda a, b: a == b,
                                    '$exists': lambda a, b: bool(a) == bool(b),
                                    '$in': lambda a, b: a in b }

                    firstkey = list(test['val'].keys())[0]
                    comparator = comparators.get(firstkey)
                    if comparator and comparator(row.get(test['key']), test['val'].get(firstkey)):
                        res.append(row)
                        break
        return res


    @classmethod
    def do_query(cls, master, clauses):
        result = master[:]
        for key, val in clauses.items():
            _t = cls.detect_clause_type(key, val)
            if _t == 'NORMAL':
                result = cls.match_rows_NORMAL(result, {'key':key,'val':val})
            elif _t == 'CONDITIONAL':
                result = cls.match_rows_CONDITIONAL(result, {'key':key,'val':val})
            elif _t == 'OR':
                result = cls.match_rows_OR(result, val)
        return result

    def find(self, match):
        return self.do_query(self._data, match)

#    update
#       do_query
#    find
#       do_query
#    distinct
#       do_query
#    remove
#       do_query
#    count
#    do_query
#        return db_result

#
#class queryable:
#    """
#    Module Entry Points
#    """
#    # can specify a path to open, and can provide data
#    @staticmethod
#    def open(db_path='', set_data=[]):
#        return db_object(db_path, set_data)


if __name__ == '__main__':
    db = db_object()
    db.insert({'the quick':1})
    db.insert({'brown':'fox'})
    db.insert({'jumped':66.6})
    db.insert({'over the':'lazy dog'})
    #v = db.find({'$or':[{'brown':{'$exists':True}},{'fred':{'$exists':True}},{'the quick':{'$gt':0}}]})
    #v = db.find({'$or':[{'fred':{'$exists':True}},{'the quick':{'$gt':0}}]})
    #v = db.find({'the quick':{'$lt':2}})
    v = db.find({'brown':{'$exists':False}})
    print(str(v))
    v = db.find({'brown':{'$exists':True}})
    print(str(v))
    db.path('test.db').save()

    db = db_object()
    print(db._data)
    db = db_object().path('test.db').load()
    print(db._data)

