
import re, os, sys
sys.path.append(os.path.realpath('.'))
sys.path.append(os.path.realpath('..'))
from Queryable import db_object

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
