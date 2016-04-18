"""
Queryable.py

OVERVIEW

* Queryable.py * is a super simplistic database system.
Essentially it is a wrapper around JSON which superimposes a
query language over an "JSON array full of objects", giving the
ability to query it in various ways using a simplified version of
MongoDB syntax.

Queryable.py is designed to be as simple as possible, adding no
features beyond the basic I/O manipulation of the JSON file,
and for everything to be included in one file with the minimum
possible number of manipulations to get your data into and out
of the JSON array.

USAGE:
from Queryable import db_object

db = db_object().path('/optional/path/to_save/or_load/from.json').data(json_array_or_string)
db.load()

res = db.find() # returns a db_result
                # db_result contains the rows matching find() parms
                # and the methods to sort().limit().skip() -- methods can be chained
db.update()
db.remove()
db.save()
"""

import json
import re
from operator import attrgetter

class db_result:
    def __init__(self, init_data):
        if type(init_data) is type([]):
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
        return self

    def sort(self, obj):
        k = list(obj.keys())[0]
        v = {'reverse':True} if obj[k] < 0 else {'reverse':False}
        self.data = list(sorted(self.data, key=lambda B:B[k], **v))
        return self

    def limit(self, ival):
        self.data = self.data[:ival]
        return self

    def skip(self, ival):
        self.data = self.data[ival:]
        return self

    def count(self):
        return len(self.data)

    def toString(self,min=False):
        xa = {} if min else {'indent':2}
        return json.dumps(self.data, **xa)


class db_object:
    """
    db_object requires a path to save
    """

    def __init__(self,compact=True, auto_index='_id'):
        self._id = 0
        self._path = ''
        self._data = []
        self._jsonarg = {} if compact else {"indent":2}
        self.auto_index = auto_index

        def verify(a,b):
            if type(a) is type(None) or type(a) is type(False):
                return False
            if type(a) is type(b):
                return True
            if type(a) is type(1000) and type(b) is type(3.14):
                return True
            if type(a) is type(3.14) and type(b) is type(1000):
                return True
            return False
        self.comparators = {'$lt': lambda a, b: verify(a, b) and a < b,
                            '$lte':lambda a, b: verify(a, b) and a <= b,
                            '$gt': lambda a, b: verify(a, b) and a > b,
                            '$gte':lambda a, b: verify(a, b) and a >= b,
                            '$ne': lambda a, b: a != b,
                            '$eq': lambda a, b: a == b,
                            '$exists': lambda a, b: bool(a) == bool(b),
                            '$in': lambda a, b: a in b,
                            '$nin': lambda a, b: a not in b}

    def path(self, _path):
        self._path = _path
        return self

    def data(self, _data):
        if type(_data) is type(''):
            self._data = json.loads(_data)
        elif type(_data) is type([]):
            self._data = _data
        else:
            raise Exception('db_object: bad input, must be list of dict or json string')
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
            if self.auto_index and self.auto_index not in row:
                row[ self.auto_index ] = self.new_index()
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

    def toString(self,min=False):
        xa = {} if min else {'indent':2}
        return json.dumps(self.data, **xa)

    def detect_clause_type(self, key, val):
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
            if k in self.comparators:
                return 'CONDITIONAL'
            return 'SUBDOCUMENT'
        elif type(val) is type([]):
            if key == '$or':
                return 'OR'
            return 'ARRAY'
        else:
            return 'UNKNOWN'

    def match_rows_NORMAL(self, rows, test):
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

    def match_rows_CONDITIONAL(self, rows, test):
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

            # these comparators positively match the key, so we can do them as a group
            # for every unique key in the row
            for key in row.keys():
                # key matches
                if key == test['key']:
                    compare = self.comparators.get(cond)
                    if compare is not None:
                        if compare(row[key], test['val'].get(cond)):
                            res.append(row)
                            break
        return res

    def match_rows_OR(self, rows, array):
        assert type(rows) is type([])
        assert type(array) is type([])
        res = []

        for row in rows:
            for elt in array:
                eltkey = list(elt.keys())[0]
                eltval = elt[eltkey]
                test = { "key": eltkey, "val": eltval }

                _t = self.detect_clause_type( eltkey, eltval )
                if _t == 'NORMAL':
                    if type(test['val']) is type(re.compile('')):
                        if row.get(test['key']) is not None and \
                                test['val'].match(str(row[test['key']])):
                            res.append(row)
                    elif row.get(test['key']) == test['val']:
                        res.append( row )
                elif _t == "CONDITIONAL":
                    firstkey = list(test['val'].keys())[0]
                    compare = self.comparators.get(firstkey)
                    if compare and compare(row.get(test['key']), test['val'].get(firstkey)):
                        res.append(row)
        return res


    def do_query(self, master, clauses):
        result = master[:]
        for key, val in clauses.items():
            _t = self.detect_clause_type(key, val)
            if _t == 'NORMAL':
                result = self.match_rows_NORMAL(result, {'key':key,'val':val})
            elif _t == 'CONDITIONAL':
                result = self.match_rows_CONDITIONAL(result, {'key':key,'val':val})
            elif _t == 'OR':
                result = self.match_rows_OR(result, val)
        return result

    def find(self, match):
        return db_result(self.do_query(self._data, match))

    def clear(self):
        self._id = 0
        self._data = []

    def update(self, constraint, operand):
        matched = self.do_query(self._data, constraint)
        return self

    def distinct(self):
        #do_query
        return self

    def remove(self, constraints):
        for matched in self.do_query(self._data, constraints):
            self._data.remove(matched)
        return self

    def count(self):
        return len(self._data)

