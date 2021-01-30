Prange class:
Page range container
Store page ranges in table
Each table should have multiple page range for each columns.
Each page range has maximum of pages (64KB), if out of 64KB,
create a new page range and append it to the last one.

// @param: page_position
// the position of the last page that we want to append the new
// page to.
func: append_page(page_position)

Record class:
Record container
A record will be innitialized when user want to insert or update 
a piece of data to the table in the database.

@param: rid = record's ID
@param: indirect = pointer to the newest data.
if points to itself, means it is a BASE record,
otherwise, TAIL record.
@param: prange_pos = WAITING RON's COMMENT
@param: page_pos = points to the location that this record been stored
in the page (which page it is).
@param: offset = the offset in the page.
@param: Record_key = in this milestone, the record_key is the student
ID
@param: columns = TUPLE of data entered by user

// TODO
// get current ID
// should return a RID
func: get_Cur_Rid()

// TODO
// get next available RID
// should just return current rid + 1
func: get_Next_availableRid()

Table class:
Table container
the most import container in the database
Table should contain 3 parts: Index part (B-tree),
Page Range (store data location info), and Record container (store
records detail information)
When a data be written into the table, this data should also been 
processed into all of these 3 parts.

@param: name = table's name
@param: Table_key = table's ID
@param: num_columns = number of columns in the table,
which teels how many categories included in the table.
@param: page_directory = dictionary that stored the page information 
in the table. HashTable liked, HashKey is the index, which represent
the category; HashValue is an array(list), which store the information
of the specific page range (category), ex: how many page is in this page
range.
@param: index = WAITING RON's COMMENT
@param: record_directory = dictionary that stored the record information
in the table. HashTable liked, HashKey is the record key (whose HashValue is
the record in the base page, or original record), and another HashKey is the 
record's RID (whose HashValue is the newest record in the tail page)
@param: prange_num = TODO
@param: free_rid = how many RID this table has used.

// get next free RID in this table
// @return: a rid
func: next_free_rid()

// create a page range for the specific column
func: create()

// create then insert a record to the next available 
// position in the base page for every column
// @param: TUPLE of data inserted by user
// @return: -1 if fail to insert, 0 if success
func: insert_record(*data)

// create then update a record to the next available
// position in the base page for every column
// @param: TUPLE of data updated by user
// @return: 0 if success
func: update_record(*data)

// append a new base page to the specific column
// should only be called at the beginning of creating
// a page range for a column or the current base page
// in the page range is full
// @param: ith_column = the target column the base page be
// inserted to
func: insert_page_to(ith_column)

// append a new tail page to the specific column
// should only be called when the current tail page in the
// target column is full
// @param: ith_column = the target column that the tail page
// be appened to
func: update_page_to(ith_column)

func: create_record