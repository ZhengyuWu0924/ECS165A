from template.db import Database

database = Database()
grades_table = database.create_table('Grades', 5, 0)

# retValue = database.get_table('Grades')
# print(retValue)
# db_length = len(database.tables_directory)
# print(db_length)

sec_grades_table = database.create_table('Grades2', 5, 0)
retNewTable = database.get_table('Grades2')
print(retNewTable)
db_length2 = len(database.tables_directory)
print("Data base length:",db_length2)
print("Table name:",sec_grades_table.name)
print("Table key",sec_grades_table.Table_key)
print("Table columns:",sec_grades_table.num_columns)
print("Table page directory:",sec_grades_table.page_directory)
print("Table index:",sec_grades_table.index)
print("Table record directory:",sec_grades_table.record_directory)
print("Table prange:",sec_grades_table.prange_num)
print("Table free base rid:",sec_grades_table.free_brid)
print("Table free tail rid:",sec_grades_table.free_trid)

print("Before calling drop", len(database.tables_directory))
database.drop_table('Grades')
print("After calling drop", len(database.tables_directory))