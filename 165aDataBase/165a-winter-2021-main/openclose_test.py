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
# newtable = db.create_table('nidaye', 5, 0)
newtable = db.get_table('nidaye')
query = Query(newtable)
keys = []

# for i in range(0, 30000):
#     query.insert(906659671 + i, 93, 0, 0, 0)
#     keys.append(906659671 + i)

# update_time_0 = process_time()
# for i in range(0, 30000):
#     up = [0, 94, 1, 1, 1]
#     query.update(906659671 + i, *up)
# update_time_1 = process_time()
# print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)

# for i in range(0, 30000):
#     query.insert(106659671 + i, 93, 0, 0, 0)
#     keys.append(106659671 + i)

# for i in range(0, 30000):
#     up = [0, 94, 2, 2, 2]
#     query.update(906659671 + i, *up)
db.close()
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
print(newtable.buffer.pool.keys())
print(newtable.buffer.trash_bin.keys())
# print(newtable.buffer.pool[0][8][0].bpage_num)
# print(newtable.buffer.pool[0][0][0].tpage_num)
print(newtable.read_record(106659671))
print(len(newtable.page_directory))


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