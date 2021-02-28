"""
A data strucutre holding indices for various columns of a table.
Key column should be indexd by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""
from BTrees.OOBTree import OOBTree
from template.config import *

class Index:
    
    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns #Btree
        self.count_rid = [0] * table.num_columns # number of rid in each column
        self.col_tree_dic = {}
        self.create_index(table.Table_key)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):

        try:
            return self.indices[column][value]
        except:
            return "KeyError"

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    # usage: list(t.values(min=1, max=4, excludemin=True, excludemax=True))
    """

    def locate_range(self, begin, end, column):
        return list(self.indices[column].values(begin, end, excludemax=False)) # return a list of lists


    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
            self.indices[column_number] = OOBTree()

    """
    # insert a new record
    """

    def insert(self, column, value, rid):
        if value is None:
            value = -MAX_LONGINT
        if self.indices[column] is not None:
            # print('first')
            self.count_rid[column] += 1
            if self.indices[column].has_key(value):
                self.indices[column][value].append(rid)
            else:
                self.indices[column][value] = [rid]
        else:
            # print('second')
            #print(column)
            self.create_index(column)
            self.count_rid[column] = 1
            self.indices[column][value] = [rid]

    def delete(self, column, value, rid):
        if value == '/':
            value = -MAX_LONGINT
        # print(self.indices[column][value])
        # print(self.indices[column][value])
        if self.indices[column].has_key(value):
            if rid in self.indices[column][value]:
                self.indices[column][value].remove(rid)
            if self.indices[column][value] == []:
                self.indices[column].__delitem__(value)
        return True

    def update(self, column, old_value, new_value, rid):
        if old_value == None:
            old_value = -MAX_LONGINT
        if new_value == None:
            new_value = -MAX_LONGINT
        # print(rid)
        self.delete(column, old_value, rid)
        self.insert(column, new_value, rid)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
