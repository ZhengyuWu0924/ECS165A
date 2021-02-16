from template.table import Table
import os
import io
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
            t_path = file + '/table.pkl'
            obj = pickle.load(t_path)
            self.tables_directory.append(obj)
            self.num_table += 1
        pass
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
            f = open(path + '/' + table.name + '.pkl', 'wb')
            table.index = None #To Do: add index.txt
            pickle.dump(table, f, True)
            f.close()
        pass

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
        
        new_table = Table(name, num_columns, key)
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
