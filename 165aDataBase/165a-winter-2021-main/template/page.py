from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.max_record_amount = 4096/RECORD_SIZE
        self.has_cap = True

    def has_capacity(self):
        if self.num_records < self.max_record_amount:
            self.has_cap = True
            return True
        self.has_cap =False
        return False
        # pass

    def write(self, value):
        cap = self.has_capacity()
        if cap == True:
            start = self.num_records * RECORD_SIZE
            end = start + RECORD_SIZE
            self.data[start:end] = value.to_bytes(RECORD_SIZE, byteorder='big')
            self.num_records += 1
        else:
            print('Page writing failed')

