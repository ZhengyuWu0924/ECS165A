from template.config import *


class Page:

    def __init__(self,page_key):
        self.num_records = 0
        self.data = bytearray(4096)
        self.max_record_amount = 4096/RECORD_SIZE
        self.has_cap = True
        self.page_key = page_key

    def has_capacity(self):
        if self.num_records < self.max_record_amount:
            self.has_cap = True
            return True
        self.has_cap =False
        return False

    def writeRecord(self, value):
        cap = self.has_capacity()
        if cap == True:
            start = self.num_records * RECORD_SIZE
            end = start + RECORD_SIZE
            self.data[start:end] = value.to_bytes(RECORD_SIZE, byteorder='big')
            self.num_records += 1
        else:
            print('Page writing failed')
            
    def readRecord(self, recordOffset):
        if recordOffset < 0 or recordOffset >= self.max_record_amount:
            print("Offset out of range")
            return -1
        start = recordOffset * RECORD_SIZE
        end = start + RECORD_SIZE
        ret = self.data[start:end]
        ret = int.from_bytes(ret, byteorder='big')
        return ret

    def get_record_amount(self):
        return self.num_records

    def get_record_data(self):
        return self.data