from template.page import *


page = Page(1)
print(page.has_capacity())
data = [55,77]
page.writeRecord(data[0])
page.writeRecord(1024)
page.writeRecord(1023)

read0 = page.readRecord(0)
read1 = page.readRecord(1)
read2 = page.readRecord(512)
print(read0)
print(read1)
print(read2)
bpage = Page(1)
tpage = Page(1)
prange = [bpage,tpage]
print(prange[0])