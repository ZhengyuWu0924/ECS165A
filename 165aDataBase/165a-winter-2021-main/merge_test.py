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

for i in range(0, 5):
    query.insert(1 + i, 93, 0, 0, 0)
    keys.append(1 + i)

# update_time_0 = process_time()
for i in range(0, 4):
    up = [0, 94, 1, 1, 1]
    query.update(1 + i, *up)

for i in range(0, 2):
    up = [0, 95, 2, 1, 1]
    query.update(1 + i, *up)
# query.update(906659671, 5, 5, 5, 5, 5)
# query.update(906659671, 5, 5, 5, 6, 5)
# query.update(906659671, 5, 2, 5, 6, 5)
records = query.select(95, 1, [1,1,1,1,1])
for record in records:    
    print(record.columns_)
# print(newtable.merge_times)
# print(newtable.merge_waiting_set)
for item in newtable.index.indices[1].iteritems():
    info = str(item[0]) + '_'
    for num in item[1]:
         info = info +  str(num) + '_'
    # print(info)
info = info.split('_')
# for rid in info[1:-1]:
#     print(int(info[0]), rid)
db.close()
# obj = newtable.page_directory.get(906659671)
# print(obj.columns_)
# print(obj.tps)

# newtable.merge(906659671)

# obj = newtable.page_directory.get(906659671)
# print(obj.columns_)
# print(obj.tps)

# page = Page(0)
# page.writeRecord(223)
# page.writeRecord(224)
# page.writeRecord(225)
# page.updateRecord(1, 226)
# print(page.readRecord(1))

#base_record.tps < base_record.update_num  haven't merged
#base_record.tps = base_record.update_num  merged


