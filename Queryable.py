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

# load from file
db = db_object().path('/optional/path/to_save/or_load/from.json').load()
    OR
db = db_object().load(path='/optional/path/to_save/or_load/from.json')

# pre-populate with data
db = db_object().data(json-array_or_json-string)

res = db.find({'key':'value'})
        # returns a db_result
        # db_result contains the rows that match find() params
        # the methods to sort().limit().skip() -- methods can be chained
# can use regex values
res = db.find({'key':re.compile('^[0-9]+value')})

db.update()
db.remove()
db.save()

TODO:
- make correct methods @staticmethod and @classmethod
- move comparators to class level 
"""

import json
import re
from operator import attrgetter
import copy
from datetime import datetime

class db_result:
    def __init__(self, init_data):
        if type(init_data) is type([]):
            self.data = copy.deepcopy(init_data)
        elif type(init_data) is type({}):
            self.data = [ copy.deepcopy(init_data) ]
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
    def __init__(self,compact=True, auto_index='_id', path=None, data=None):
        """
        compact:    set to false to save JSON in multi-line format for readability
        auto_index: can be unset to eliminate automaticly adding indexes into objects

        3 ways to get data into db:
            - data() - passes in raw and doesn't alter fields (won't inject _id)
            - insert() - inserts each row, altering as normal (may inject _id)
            - load() - loads from file. Read highest _id so subsequent inserts start at next

        """
        self._id = 0
        self._path = path if path else ''
        self._data = data if data else []
        self._jsonarg = {'separators':(',',':')} if compact else {"indent":2}
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
        """
        set the file path. must be fully qualified file path, not just directory
        """
        if _path:
            self._path = _path
        return self

    def data(self, _data):
        """
        populate the database with data. Can be both/either string or ``list of dict''
        """
        if _data is not False:
            if type(_data) is type(''):
                self._data = json.loads(_data)
            elif type(_data) is type([]):
                self._data = _data
            else:
                raise Exception('db_object: bad input, must be list of dict or json string')
        if self.auto_index and self._data:
            for row in self._data:
                if self.auto_index not in row:
                    row[ self.auto_index ] = self.new_index()
        return self

    def load(self, path=False):
        """
        load from file.

        path: Can optionally set the filepath
        """
        self.path(path)
        with open(self._path, 'r') as f:
            self._data = json.load(f)
        for x in [row.get('_id') for row in self._data if row.get('_id') is not None]:
            if x > self._id:
                self._id = x
        return self

    def save(self, path=False, compact=None):
        """
        save to file.

        path:       Can optionally set the filepath
        compact:    set to true or false to control how save operation writes the file
        """
        self.path(path)
        if compact is not None:
            self._jsonarg = {'separators':(',',':')} if compact else {"indent":2}
        with open(self._path, 'w') as f:
            f.write( json.dumps(self._data, **self._jsonarg) )
        return self

    def insert(self, row_or_ary):
        """
        insert a row (dict), or array or rows (list of dict)
        """
        def insert_one(self, row):
            assert type(row) is type({})
            if self.auto_index and self.auto_index not in row:
                row[ self.auto_index ] = self.new_index()
            self._data.append(row)

            # replace any 'now()' value strings with the current timestamp
            if 'now()' in row.values():
                for k, v in row.items():
                    if v == 'now()':
                        row[k] = datetime.strftime(datetime.now(),'%Y-%m-%dT%H:%M:%S.%f%z')

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
        result = master
        for key, val in clauses.items():
            _t = self.detect_clause_type(key, val)
            if _t == 'NORMAL':
                result = self.match_rows_NORMAL(result, {'key':key,'val':val})
            elif _t == 'CONDITIONAL':
                result = self.match_rows_CONDITIONAL(result, {'key':key,'val':val})
            elif _t == 'OR':
                result = self.match_rows_OR(result, val)
        return result

    def find(self, match=None):
        """
        return all rows matching query
        """
        if match is None:
            return db_result(self._data)
        return db_result(self.do_query(self._data, match))

    def clear(self):
        """
        erase the internal database
        """
        self._id = 0
        self._data = []
        return self

    def remove(self, constraints):
        """
        remove all rows from database that match the query
        """
        for matched in self.do_query(self._data, constraints):
            self._data.remove(matched)
        return self

    def update(self, query, update, options=None):
        """
        update all items matching query,
        set their keys to any in update object, as: {"$set":{key:val}}
        set multi:True to update multiple rows: options => {'multi':True}
        set upsert:True to insert if row query is missing => {'upsert':True}
        """
        do_upsert = True if options and options.get('upsert') else False
        do_multi = True if options and options.get('multi') else False
        _set = update['$set'] # error if doesn't have it
        matched = self.do_query(self._data, query)
        if len(matched) == 0 and do_upsert:
            self.insert(_set)
            return self
        did_change = False
        for row in matched:
            for key, val in _set.items():
                if not row.get(key) or row[key] != val:
                    row[key] = val
                    did_change = True
            if not do_multi and did_change:
                break # default is do only one row
        return self

    def distinct(self, key, clause=None):
        """
        return only distinct set of key:value pairs possessing 'key'
        where no more than one row of key:value is returned
        """
        assert type(key) is type('')

        if clause is not None:
            res = self.do_query(self._data, clause)
        else:
            res = self._data

        # thin out to every row that has the key
        haskey = []
        for row in res:
            if row.get(key) is not None:
                haskey.append(row)

        distinct_set = []
        for keyrow in haskey:
            not_in = True
            for distinct_row in distinct_set:
                if distinct_row[key] == keyrow[key]:
                    not_in = False
                    break
            if not_in:
                distinct_set.append(keyrow)

        return db_result(distinct_set)

    def count(self):
        return len(self._data)

