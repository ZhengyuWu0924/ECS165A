# -*- codeing = utf-8 -*-
# @Time : 2021/1/31 12:01 上午
# @Author : Tedder Lao
# @File: index_test.py
# @Software: PyCharm

from template.table import *
from template.index import *

table = Table('Grades', 3, 0)
table.create()

I = Index(table)
print(I)

I.insert(0, None, 2)
I.update(0,None,87,2)
print(I.locate(0,87))
# I.update(0, 87, None, 2)
# print(I.delete(0, 87, 2))
# print(I.locate(0,87))







