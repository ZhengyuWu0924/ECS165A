from template.table import Table

class Database():

    def __init__(self):
        self.tables_directory = []
        self.num_table = 0

    """
    Not for MS1
    Implement this in future
    SSD or HDD related
    Open a table from Secondary memory
    """
    def open(self, path):
        pass
    """
    Not for MS1
    Implement this in future 
    SSD or HDD related
    Close a table
    """
    def close(self):
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
    * In MS1, we assume mutiple tables with the same properties are 
    not allowed in the database
    """
    def create_table(self, name, num_columns, key):
        
        for table in self.tables_directory:
            if name in table.name:
                if num_columns in table.num_columns:
                    if key in table.key:

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
