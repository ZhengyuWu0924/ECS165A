from template.table import Table
from template.index import Index
from template.bufferpool import Bufferpool
import os
# import io
import pickle

class Database():

    def __init__(self):
        self.tables_directory = []
        self.num_table = 0
        self.path = ''

    """
    Not for MS1
    Implement this in future
    SSD or HDD related
    Open a table from Secondary memory
    """
    def open(self, path):
        self.path = path
        if not os.path.isdir(path):
            os.mkdir(path)
            return
        for file in os.listdir(path):
            # print(file)
            t_path = self.path + '/' + str(file) + '/' + str(file) + '.pkl'
            # print(t_path)
  
            f = open(t_path, 'rb+')
            table = pickle.load(f)
            f.close()
            table.index = Index(table)
            table.buffer = Bufferpool(table)
            # obj.index.create_index(0)
            for i in range(table.num_index):
                index_path = self.path + '/' + file + '/table_index_col' + str(i) + '.txt'
                indexObj = open(index_path, 'r+')
                for line in indexObj.readlines():
                    line = line.split('_')
                    for rid in line[1: -1]:
                        table.index.insert(i, int(line[0]), rid)
                indexObj.close()
            # self.tables_directory.append(obj)
            self.append_table(table)
            # print(len(self.tables_directory))
            self.num_table += 1
    """
    Not for MS1
    Implement this in future 
    SSD or HDD related
    Close a table
    """
    def close(self):
        for table in self.tables_directory:
            path = self.path + '/' + table.name
            if not os.path.isdir(path):
                os.mkdir(path)
            # store user data
            dataAddr = path + '/' + 'disk.txt'
            dataFile = open(dataAddr, 'w')
            for recordRid in table.rid_list:
                record = table.read_record(recordRid)
                dataFile.write(str(record) + '\n')
            dataFile.close()
            # store index
            cnt = 0
            for i in range(len(table.index.indices)):
                
                if table.index.indices[i] is not None:
                    cnt += 1
                    indexFileAddress = path + '/' + 'table_index_col' + str(i) +'.txt'
                    indexFile = open(indexFileAddress, 'w')
                    for node in table.index.indices[i].iteritems():
                        info = str(node[0]) + '_'
                        for num in node[1]:
                            info = info + num +'_'
                        indexFile.write(info + '\n')
                    indexFile.close()
            table.num_index = cnt
            # store table info
            f = open(path + '/' + table.name + '.pkl', 'wb')
            ### store page_range
            # os.mkdir(path + '/' + 'Data')
            # for num in range(len(table.prange_directory[0])):
            #     for prange in table.prange_directory.items():
            #         f_p = open(path + '/' + 'Data' + '/' + 'prange' + str(num) + '_' + str(prange[0]) + '.pkl', 'wb')
            #         pickle.dump(prange[1], f_p, True)
            #         f_p.close()
            table.index = None
            table.buffer.cleanBin()
            table.buffer = None
            # table.prange_directory = {}
            pickle.dump(table, f, True)
            self.drop_table(table.name)
            f.close()

    """
    Append a table to database
    :param: table               #A table that going to be appended 
    Private function
    only be called in this class
    """
    def append_table(self, table):
        self.tables_directory.append(table)
    
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    :return the table just created
    * In MS1, we assume mutiple tables with the same name are 
    not allowed in the database
    """
    def create_table(self, name, num_columns, key):
        
        for table in self.tables_directory:
            if name in table.name:
                print("Already exists such a table")
                print("Returning this table")
                return table
        os.mkdir(self.path + '/' + name)
        new_table = Table(name, num_columns, key, self.path)
        new_table.create()
        self.append_table(new_table)
        self.num_table += 1
        return new_table

    """
    # Deletes the specified table
    IF we should also free the memory when dropping?
    :param name = table to drop from directory
    :return -1 if no such table in directory or fail to drop
    :return 0 if success
    """
    def drop_table(self, name):
        for table in self.tables_directory:
            if name not in table.name:
                continue
            else:
                self.tables_directory.remove(table)
                return 0
        print("No such table in directory")
        return -1

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables_directory:
            if name not in table.name:
                continue
            else:
                ret_table = table
                return ret_table
        print("Fail to get table")
        return -1
