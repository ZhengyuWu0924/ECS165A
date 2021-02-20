from template.table import *
import pickle
import io
import os
from template.query import *
from random import choice, randint, sample, seed
from template.db import Database
from time import process_time

table = Table('Grades', 5, 0)
table2 = Table('Grades', 5, 1)
# print(table)
table.create()
table2.create()
query = Query(table)
query2 = Query(table2)
records = {}

test_list = [[9999, None, 2 ,None, 4],
            [9998, 1, 2, 4, 3],
            ]

update_data = [9999, 4, 3, None, 1]
update_data2 = [9999, 4, 2, None, 1]
update_data3 = [9990, 4, 2, None, 2]
update_data4 = [9999, None, None, None, None]

# query.update(9999, *update_data4)
# for i in range(0, 30000):
#     key = 92106429 + randint(0, 100000)
#     while key in records:
#         key = 92106429 + randint(0, 9000)
#     records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
#     query2.insert(*records[key])
#     if i == 25000:
#         data = table2.read_record(key)
# print(data)
# print(table2.prange_num)
# # print(table2.rid_list)
# print(len(table2.page_directory.get(0)))

# for i in test_list:
#     query.insert(*i)
# #     # table.update_record(*update_data)
# #     data = table.read_record(9999)
# #     print(table.get_rid_list())
# # data = table.read_record(9999)
# # table.update_record(*update_data3)
query.update(9999, *update_data) #[9999, 4, 3, None, 1]
query.update(9999, *update_data2) #[9999, 4, 2, 4, 1]
query.update(9990, *update_data3) #[9990, 4, 2, None, 2]
query.update(9999, *update_data4) #[9999, None, None, None, None]
# value = query.sum(4, 5, 1)
# # data = table.read_record(9999)
# print(value)
table.__merge(9999)
print('1111')

"""
db = Database()
db.open('./ECS165')
os.mkdir('./ECS165/table1')
os.mkdir('./ECS165/table2')
f = open('./ECS165/table1/table1.pkl', 'wb')

# f = open('./ECS165/table1/table.pkl')
grades_table = db.create_table('table1', 5, 0)
# grades_table.index = None
query = Query(grades_table)
keys = []

insert_time_0 = process_time()
for i in range(0, 10):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)
# insert_time_1 = process_time()
indexFile = open('./ECS165/table1/table1_index.txt', 'w')
for node in grades_table.index.indices[0].iteritems():
    # indexFile.write(str(node.key()))
    key = node[0]
    # print(key)
    indexFile.write(str(key) + "+")
    indexFile.write(str(node[1][0]))
    indexFile.write('\n')
indexFile.close()

# readIndex = open('./ECS165/table1/table_index.txt', 'r')
# grades_table.index.create_index(0)
# for line in readIndex.readlines():
#     print(line.split('+')[0])
#     print(line.split('+')[1][:-1])
#     grades_table.index.insert(0, int(line.split('+')[0]), line.split('+')[1][:-1])

# print(grades_table.index.locate(0, 906659672))



# readIndexFile = open('./ECS165/table1/table_index.txt', 'w+')
# print(readIndexFile.readline())
# print(readIndexFile.readline().split(',')[0][1:])
# print(readIndexFile.readline().split(',')[1][0:])

grades_table.index = None
pickle.dump(grades_table, f, True)
f.close()

# f = open('./ECS165/table1/table.pkl', 'rb')
# table = pickle.load(f)
# print(table.name)

# f.close()
db.close()


db.open('./ECS165')
print("open")
# print(grades_table.page_directory.get(906659671))
# f = open('test.pkl', 'wb')
# pickle.dump(grades_table.page_directory, f,True)
# print(a)
# f.close()
# print(grades_table.index.indices[0])

# f = open('test.pkl', 'rb')
# record = pickle.load(f)
# print(record.get(906659671))

# f = os.mkdir('./ECS165')
# f = os.path.isdir('./ECS165')
# f = os.listdir('./ECS165')
# print(f)


# print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)
# print(query.table.prange_num)
"""