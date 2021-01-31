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

I.insert(0, 87, 'g')
I.insert(0, 87, 2)
I.insert(0, 50, 3)
I.insert(0, 100, 4)
I.insert(0, 82, 5)
I.insert(1, 82, 55)
#I.delete(0, 87 ,2)
I.update(0, 87, 82, 'g')
print(I.locate(0,87))
print(I.locate(0,82))
print(I.locate_range(60, 90, 0))





