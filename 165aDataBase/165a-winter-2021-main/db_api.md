Database class:
Database container
Each database is able to included multiple tables.
However, no multiple tables with exactly the same properties
allowed.
If user wants to create a new table with exactly the same name,
num_columns, and key, the database will tell there is already such
a table exists, and return that table back to user.

@param: tables_directory = container to store tables
@param: num_table = record how many tables are there in the database

// Open function
// give a path, open the specific table in this path
// No physical disk used in MS1
// Implement this in future
// @param: path = a path points to the location in physical
// disk
func open(path)

// Close function
// save the database to the physical disk and close it
// no physical disk involved in MileStone 1
// implement this in future
func close()

// Append table function
// Private function - should only be called in this class
// append a table to databse
// @param: table = table waittede to be appeneded
func append_table(table)

// Create table function
// the most important function in this class
// check if there is already a table with exactly same name in 
// databse
// if yes, return the already existed one and tell the user the creating
// fail
// if no, create a new table and append it to the database
// @param: name = table name provided by user
// @param: num_columns = amount of columns in in this table, 
// provided by user.
// @param: key = table key, provided by user
// @return: table = already existed table
// @return: new_table = a new table just created and appened to database
func create_table(name, num_columns, key)

// Drop table function
// deallocate a table from the current database
// *Not sure if we should also free the memory this table take
// @param: name = the name of a table we want to drop
// @return: 0 successful delete, -1 otherwise
func drop_table(name)

// Get table function
// get the location of a table from the current database
// @param: name = the name of the table to get
// @return: ret_table = the table gonna be returned
// @return: -1 = fail to get the table
func get_table(name)