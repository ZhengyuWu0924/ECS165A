from template.table import Table, Record
from template.index import Index
from template.config import *


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key, undo = False):
        data = [None, None, None, None, None]
        brid = self.table.index.locate(0, key)
        record = self.table.update_record(key, brid[0], *data, delete = True)
        # print(record.schema)
        if record != 0 and record.columns_[0] == None:
            # print('succesful deletion')
            return True
        print('fail deletion')
        return False
        # If record exist in table
        # Delete in both table and bTree
        # if key in self.table.page_directory:
        #     # Delete 2 base records in table
        #     baseRecord = self.table.page_directory.pop(key, None)
        #     self.table.page_directory.pop(baseRecord.rid, None)
            
        #     # If no tail records
        #     if baseRecord.indirect != baseRecord.rid:
        #         newestTemp = self.table.read_record(baseRecord.indirect)
        #         baseRecord.columns_ = newestTemp
            
        #     # Delete value in each column
        #     for index in range(len(baseRecord.columns)):
        #         deleteVal = None
        #         if baseRecord.columns[index] == '/' or baseRecord.columns[index] is None:
        #             deleteVal = -MAX_LONGINT
        #         deleteVal = baseRecord.columns_[index]
        #         self.table.index.delete(index, deleteVal, baseRecord.rid)
        #     # Successful delete
        #     return True
        # # If record doesn't exist
        # return False
       
        #pass

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns, undo = False):
        if columns == None:
            return False
        if self.table.insert_record(*columns) != False:
            return True
        return False
        # *columns = [key, grade, grade, grade, grade]
        # The​ insert function will insert a new record in the table
        schema_encoding = '0' * self.table.num_columns
        # Should schema encoding insert into the columns?
        # self.table.append(*columns)


    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns, undo = False):
        recordArr = []
        # if key in self.table.page_directory:
            # get the base rid from table by bTree
        bTreeRIDs = self.table.index.locate(column, key)
        # print(column, key, bTreeRIDs)
        if isinstance(bTreeRIDs, list):    
            for baseRid in bTreeRIDs:
                # base record, used later
                ridRecord = self.table.page_directory.get(baseRid)
                # if only one base record
                if ridRecord.indirect == baseRid:
                    # tempRecord = []
                    # for index in range(len(query_columns)):
                    #     # if query_columns[index] == 1:
                    #     #     tempRecord.insert(len(tempRecord), ridRecord.columns[index])
                    #     if query_columns[index] == 1:
                    #         tempRecord.insert(len(tempRecord), ridRecord)
                    # recordArr.insert(len(recordArr), tempRecord)
                    # recordArr.insert(len(recordArr), ridRecord)
                    recordArr.append(ridRecord)
                    # return the target columns
                    return recordArr
                # set a flag to indicate circle linkedlist
                circleFlag = ridRecord.indirect
                # base record will indirect to the newest tail
                # travel reverse to form the logbook 
                prevRecord = self.table.page_directory.get(ridRecord.indirect)
                prevIndirect = prevRecord.indirect
                # whlie there is not a circle
                # while prevIndirect != circleFlag:
                #     # add reacord to the front of the log book
                #     currentRecord = prevRecord
                #     prevRecord = self.table.page_directory.get(currentRecord.indirect)
                #     recordArr.insert(len(recordArr), currentRecord)
                #     prevIndirect = prevRecord.indirect
                # circle found
                # add base record to the front
                # base_record = []
                # for index in range(len(query_columns)):
                #     # if query_columns[index] == 1:
                #     #     base_record.insert(len(base_record),
                #     #                     ridRecord.columns[index])
                #     if query_columns[index] == 1:
                #         base_record.insert(len(base_record),
                #                         ridRecord)
                # recordArr.insert(len(recordArr), ridRecord)
                recordArr.append(prevRecord)
                # return list of records
                # print(recordArr[0])
            return recordArr
        # if something wrong, return false
        # print(key)
        # print('selecting falied, key is not found or record is deleted/updated')
        return False
        

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
     # Ex:query.update(choice(keys), *(choice(update_cols)))
    def update(self, key, *columns, undo = False):
      # Record_key or table_key ?
        # print(columns[0])
        bTreeRIDs = self.table.index.locate(0, key)
        # print(type(bTreeRIDs[0]))
        if isinstance(bTreeRIDs, list): 
            # print('up')
            if self.table.update_record(key, bTreeRIDs[0], *columns, delete = False) != False:
                return True
            else:
                # print('158 up failed')
                return False
        print('up failed')
        return False
    
    # def write(self, )

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        # Ex: query.sum(i, 100, randrange(0, 5))
        # Summation of
        # 
        rid_list = self.table.index.locate_range(start_range, end_range, 0)
        # print(rid_list)
        if rid_list == None:
             return False
        sum = 0
        for rid in rid_list:
            rid_ = rid[0]
            value = self.table.read_record(rid_)[aggregate_column_index]
            sum += value
        return sum

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.Table_key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

