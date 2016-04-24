
import re, os, sys
sys.path.append(os.path.realpath('.'))
sys.path.append(os.path.realpath('..'))
from Queryable import db_object
p = lambda x: print(str(x))

#OR
db = db_object(auto_index='').insert([{'a':1},{'b':2},{'c':3},{'a':4}])
r = re.compile('.*')
res = db.find({'$or':[{'a':r},{'b':2}]})
print( res.data )

#AND
M = {'a':1,'b':2,'c':3,'d':4}
db = db_object(auto_index='').insert(M)
res = db.find(M)
print( res.data )

#IN
db = db_object(auto_index='').insert([{'a':3},{'a':5},{'b':2},{'a':1}])
res = db.find({'a':{'$in':[2,3,5]}})
print( res.data )

# EXISTS : false
db = db_object().insert({'a':1,'b':2}).insert({'b':2,'c':3}).insert({'c':3,'d':4})
print( db.find({'b':{'$exists':False}}).data, end='' )
print( ' == ' + str([{'c': 3, 'd': 4, '_id': 3}]) )

# REMOVE
db = db_object(auto_index='').insert([{'a':3},{'a':5},{'b':2},{'a':1}])
res = db.remove({'a':{'$exists':True}})._data
print(res)
db = db_object(auto_index='').insert([{'a':3},{'a':5},{'b':2},{'a':1}])
res = db.remove({'a':{'$exists':False}})._data
print(res)

# tests if it can filter out bad types
db = db_object(auto_index='').insert([{'x':1,'n':'one'},{'x':1.5,'n':'one point five'},{'x':'two','n':2}])
res = db.find({'x':{'$gt':1}})
p(res.data)

# can we sort around bad types?
db = db_object(auto_index='').insert([{'a':'a'},{'a':'A'},{'a':'c'},{'a':'2'}])
res = db.find({'a':re.compile('.*')}).sort({'a':-1})
p(res.data)

# regex
db = db_object(auto_index='').insert([{'x':'fred'},{'x':'dead'},{'x':256},{'x':'leded'}])
res = db.find({'x':re.compile('[0-9]+')})
p(res.data)
res = db.find({'x':re.compile('\d+$')})
p(res.data)
res = db.find({'x':re.compile('[a-z]{5}')})
p(res.data)
res = db.find({'x':re.compile('^[a-z]{4}$')})
p(res.data)

# update
db = db_object().data([{'x':1},{'x':1.0}])
res = db.find({'x':re.compile('.*')})
p(res.data)
db = db_object().data([{'x':1},{'x':1.0}])
db.update({'x':1},{'$set':{'x':666}})
res = db.find({'x':re.compile('.*')})
p(res.data)
db = db_object().data([{'x':1},{'x':1.0}])
db.update({'x':1},{'$set':{'x':666}},{'multi':True})
res = db.find({'x':re.compile('.*')})
p(res.data)
db.update({'b':1},{'$set':{'b':777}},{'upsert':False})
p(db._data) # [{'x':666,'_id':1},{'x':666,'_id':2}])
db.update({'b':1},{'$set':{'b':777}},{'upsert':True})
p(db._data) # [{'x':666,'_id':1},{'x':666,'_id':2},{'b':777,'_id':3}])

# distinct
M = [{'a':1},{'a':0},{'a':1},{'a':0}]
db = db_object(auto_index='').data( M )
res = db.distinct('a')
p(db._data)
p(res.data) # [{'a':1},{'a':0}])
res.data[0]['a'] = 666
p(db._data) # M) # make sure changing result isn't the same python object as main set
p(res.data) # [{'a':666},{'a':0}])

# sort dates
db = db_object(jsonarg=True) # human readable
db.insert({'x':1, 'd':'now()'})
db.insert({'x':2, 'd':'now()'})
db.insert({'x':3, 'd':'now()'})
db.insert({'x':4, 'd':'now()'})
res = db.find().sort({'d':-1})
p(res.toString())
db.save(path='./save_file')
