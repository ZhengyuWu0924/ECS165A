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
    # internal Method.
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key):
        # # If record exist in table
        # # Delete in both table and bTree
        # if key in self.table.record_directory:
        #     # Delete 2 base records in table
        #     baseRecord = self.table.record_directory.pop(key, None)
        #     self.table.record_directory.pop(baseRecord.rid, None)
            
        #     # If no tail records
        #     if baseRecord.indirect != baseRecord.rid:
        #         newestTemp = self.table.read_record(baseRecord.indirect)
        #         baseRecord.columns = newestTemp
            
        #     # Delete value in each column
        #     for index in range(len(baseRecord.columns)):
        #         deleteVal = None
        #         if baseRecord.columns[index] == '/' or baseRecord.columns[index] is None:
        #             deleteVal = -MAX_LONGINT
        #         deleteVal = baseRecord.columns[index]
        #         self.table.index.delete(index, deleteVal, baseRecord.rid)
        #     # Successful delete
        #     return True
        # # If record doesn't exist
        # return False
       
        #pass
        if self.table.delete_record(key)!= False:
            return True
        else:
            return False

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
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
    def select(self, key, column, query_columns):
        recordArr = []
        if key in self.table.record_directory:
            # get the base rid from table by bTree
            bTreeRIDs = self.table.index.locate(column, key)
            
            for baseRid in bTreeRIDs:
                # base record, used later
                ridRecord = self.table.record_directory.get(baseRid)
                # if only one base record
                if ridRecord.indirect == baseRid:
                    # tempRecord = []
                    # for index in range(len(query_columns)):
                    #     # if query_columns[index] == 1:
                    #     #     tempRecord.insert(len(tempRecord), ridRecord.columns[index])
                    #     if query_columns[index] == 1:
                    #         tempRecord.insert(len(tempRecord), ridRecord)
                    # recordArr.insert(len(recordArr), tempRecord)
                    recordArr.insert(len(recordArr), ridRecord)
                    # return the target columns
                    return recordArr
                # set a flag to indicate circle linkedlist
                circleFlag = ridRecord.indirect
                # base record will indirect to the newest tail
                # travel reverse to form the logbook 
                prevRecord = self.table.record_directory.get(ridRecord.indirect)
                prevIndirect = prevRecord.indirect
                # whlie there is not a circle
                while prevIndirect != circleFlag:
                    # add reacord to the front of the log book
                    currentRecord = prevRecord
                    prevRecord = self.table.record_directory.get(currentRecord.indirect)
                    # currentColumns = []
                    # for index in range(len(query_columns)):
                    #     # if query_columns[index] == 1:
                    #     #     currentColumns.insert(len(currentColumns),
                    #     #     currentRecord.columns[index])
                    #     if query_columns[index] == 1:
                    #         currentColumns.insert(len(currentColumns),
                    #         currentRecord)
                    recordArr.insert(len(recordArr), currentRecord)
                    prevIndirect = prevRecord.indirect
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
                recordArr.insert(len(recordArr), ridRecord)
                # return list of records
                # print(recordArr[0])
                return recordArr
        # if something wrong, return false
        return False
        

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
     # Ex:query.update(choice(keys), *(choice(update_cols)))
    def update(self, key, *columns):
      # Record_key or table_key ?
        # print(columns[0])
        if self.table.update_record(key, *columns) != False:
            return True

        else:
            return False
       
        #pass

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

