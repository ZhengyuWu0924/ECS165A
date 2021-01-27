from template.page import *


page = Page()
print(page.has_capacity())
page.writeRecord(255)
page.writeRecord(1024)
page.writeRecord(1023)

read0 = page.readRecord(0)
read1 = page.readRecord(1)
read2 = page.readRecord(512)
print(read0)
print(read1)
print(read2)