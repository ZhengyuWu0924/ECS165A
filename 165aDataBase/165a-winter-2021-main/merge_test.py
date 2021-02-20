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



# keys.append(906659671 + i)
# indexFile = open()
# for node in newtable.index.indices[0].iteritems():
#     # indexFile.write(str(node.key()))
#     key = node[0]
#     # print(key)
#     indexFile.write(str(key) + "+")
#     indexFile.write(str(node[1][0]))
#     indexFile.write('\n')
# db.close()

# print(newtable.buffer.pool[0][8][0].bpage_num)
# print(newtable.buffer.pool[0][0][0].tpage_num)



# db.open('./ECS165')
# getTable = db.get_table('nidaye')
# # print(getTable.read_record(906659671))
# print(len(getTable.prange_directory))

"""
Print file
"""
# readFile = open('./ECS165/table1/table1_index.txt', 'r')
# for line in readFile.readlines():
#     print(line.split('+')[0])
#     print(line.split('+')[1][:-1])