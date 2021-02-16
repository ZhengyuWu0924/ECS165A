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
newtable = db.create_table('table1', 5, 0)
query = Query(newtable)
keys = []

for i in range(0, 10):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)

# indexFile = open()
# for node in newtable.index.indices[0].iteritems():
#     # indexFile.write(str(node.key()))
#     key = node[0]
#     # print(key)
#     indexFile.write(str(key) + "+")
#     indexFile.write(str(node[1][0]))
#     indexFile.write('\n')
db.close()


db.open('./ECS165')
getTable = db.get_table('table1')
print(getTable.read_record(906659671))

"""
Print file
"""
# readFile = open('./ECS165/table1/table1_index.txt', 'r')
# for line in readFile.readlines():
#     print(line.split('+')[0])
#     print(line.split('+')[1][:-1])