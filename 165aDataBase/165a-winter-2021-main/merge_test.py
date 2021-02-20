from template.table import *
import pickle
import io
import os
from template.query import *
from random import choice, randint, sample, seed
from template.db import Database
from time import process_time


db = Database()
db.open('./ECS165')
newtable = db.create_table('nidaye', 5, 0)
query = Query(newtable)
keys = []

for i in range(0, 30000):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)

update_time_0 = process_time()
for i in range(0, 30000):
    up = [0, 94, 1, 1, 1]
    query.update(906659671 + i, *up)

obj = newtable.page_directory.get(906659671)
print(obj.columns_)
print(obj.tps)

newtable.merge(906659671)

obj = newtable.page_directory.get(906659671)
print(obj.columns_)
print(obj.tps)

#base_record.tps < base_record.update_num  haven't merged
#base_record.tps = base_record.update_num  merged


