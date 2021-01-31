from template.table import *
from template.query import *
from random import choice, randint, sample, seed


table = Table('Grades', 5, 0)
# print(table)
table.create()
query = Query(table)
records = {}

test_list = [[9999, 1, 2 ,None, 4],
            [9998, 1, 2, 4, 3],
            ]

update_data = [9999, 4, 3, None, 1]
update_data2 = [9999, 4, 2, None, 1]
update_data3 = [9990, 4, 2, None, 2]
update_data4 = [9999, None, None, 2, None]

# query.update(9999, *update_data4)
# for i in range(0, 1000):
#     key = 92106429 + randint(0, 9000)
#     while key in records:
#         key = 92106429 + randint(0, 9000)
#     records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
#     table.insert_record(*records[key])
#     data = table.read_record(key)
#     # print(data)

for i in test_list:
    # print(i)
    query.insert(*i)
    # table.update_record(*update_data)
    # data = table.read_record(9998)
    # print(table.get_rid_list())
# data = table.read_record(9998)
# table.update_record(*update_data3)
query.update(9999, *update_data)
query.update(9999, *update_data2)
query.update(9990, *update_data3)
query.update(9999, *update_data4)
data = table.read_record(9999)

print(data)
