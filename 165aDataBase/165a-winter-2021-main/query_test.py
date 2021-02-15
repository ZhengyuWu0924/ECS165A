from template.table import *
from template.query import *
from random import choice, randint, sample, seed


table = Table('Grades', 5, 0)
table2 = Table('Grades', 5, 1)
# print(table)
table.create()
table2.create()
query = Query(table)
query2 = Query(table2)
records = {}

test_list = [[9999, 1, None,None, None],
            [9998, 1, 2, 4, 3]
            ]

update_data = [None, None, None, None, None]
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

for i in test_list:
    query.insert(*i)
#     # table.update_record(*update_data)
#     data = table.read_record(9999)
#     print(table.get_rid_list())
# data = table.read_record(9999)
# table.update_record(*update_data3)
query.update(9999, *update_data) #[9999, 4, 3, None, 1]
query.update(9999, *update_data2) #[9999, 4, 2, 4, 1]
query.update(9990, *update_data3) #[9990, 4, 2, None, 2]
query.update(9999, *update_data4) #[9999, None, None, 2, None]


# query.delete(9999)
data = table.read_record(9999)
print(data)
print(table.page_directory[9999].schema)

# print(data)
# data2 = query.select(9999, 0, [1, 1, 1, 1, 1])
# # print(data2)
# data3 = query.increment(9999, 0)
# print(data3)
# query.delete(9999)
# data = query.select(9999, 0, [1,1,1,1,1])
# print(data[0].columns)

# file = open('fileTester.txt', 'a')
# file.writelines(str(data))
# file.write('\n')
# file.writelines(str(table))
# file.close()