from template.table import *
from random import choice, randint, sample, seed


table = Table('Grades', 5, 0)
print(table)
table.create()
records = {}

test_list = [[9999, 1, 2 ,3, 4],
            [9998, 1, 2, 4, 3],
            ]

update_data = [9999, 4, 3, 2, 1]
update_data2 = [9999, 4, 2, 2, 1]
update_data3 = [9999, 4, 2, None, 2]
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
    table.insert_record(*i)
    # table.update_record(*update_data)
    # data = table.read_record(9998)
    # print(table.get_rid_list())
# data = table.read_record(9998)
table.update_record(*update_data)
table.update_record(*update_data2)
table.update_record(*update_data3)
# data = table.read_record(9999)

# print(data)
