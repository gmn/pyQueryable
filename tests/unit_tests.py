
import re
from queryable import db_object

def objeq( o, q ):
    c = set(o.items()) & set(q.items())
    if len(c) == len(o) and len(o) == len(q):
        return True
    return False


db = db_object()
db.insert({'a':1,'b':2})
db.insert({'b':2,'c':3})
db.insert({'c':3,'d':4})

