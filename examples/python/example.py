import crossdb

conn = crossdb.connect(database=":memory:")
cursor = conn.cursor()
cursor.execute("CREATE TABLE student (name CHAR(16), age INT, class CHAR(16))")
cursor.execute("INSERT INTO student (name,age,class) VALUES ('jack',10,'3-1'), ('tom',11,'2-5')")
print ("insert rows: ", cursor.affected_rows)	
cursor.execute("SELECT * from student")
print ("select row count: ", cursor.rowcount)
names = [description[0] for description in cursor.description]
print ("columns: ", names)
print ("rows:")
for row in cursor:
	print (row)
cursor.close()
#cursor.execute("SHELL")
