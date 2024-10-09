import ctypes
import datetime
import os
import platform

def connect (host=None, port=7777, user='admin', password='admin', database=None):
	if host == None:
		return CrossDbConnection(database)
	return CrossDbConnector(host, port, user, password, database)

def load_crossdb():
	sys = platform.system()
	if sys == 'Linux':
		if os.path.exists('./libcrossdb.so'):
			return ctypes.cdll.LoadLibrary('./libcrossdb.so')
		else:
			return ctypes.cdll.LoadLibrary('libcrossdb.so')
	elif sys == 'Windows':
		if os.path.exists('./libcrossdb.dll'):
			return ctypes.cdll.LoadLibrary('./libcrossdb.dll')
		else:
			return ctypes.cdll.LoadLibrary('libcrossdb.dll')

class CrossDbType(object):
	XDB_TYPE_TINYINT	= 1
	XDB_TYPE_SMALLINT	= 2
	XDB_TYPE_INT		= 3
	XDB_TYPE_BIGINT	 = 4
	XDB_TYPE_FLOAT	   	= 9
	XDB_TYPE_DOUBLE	 = 10
	XDB_TYPE_CHAR		= 12

class CrossDbInterface(object):
	libCrossdb = load_crossdb()
	libCrossdb.xdb_open.argtypes = [ctypes.c_void_p]
	libCrossdb.xdb_open.restype = ctypes.c_void_p
	libCrossdb.xdb_close.argtypes = [ctypes.c_void_p]
	libCrossdb.xdb_exec.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
	libCrossdb.xdb_exec.restype = ctypes.c_void_p
	libCrossdb.xdb_fetch_row.argtypes = [ctypes.c_void_p]
	libCrossdb.xdb_fetch_row.restype = ctypes.c_void_p
	libCrossdb.xdb_free_result.argtypes = [ctypes.c_void_p]
	libCrossdb.xdb_commit.argtypes = [ctypes.c_void_p]
	libCrossdb.xdb_rollback.argtypes = [ctypes.c_void_p]

class CrossDbConnection(object):
	def __init__(self, database):
		self._hConn = ctypes.c_void_p()
		if CrossDbInterface.libCrossdb == None:
			self._hDb == None
			print ("Can't open CrossDb library")
			return
		self._hConn = CrossDbInterface.libCrossdb.xdb_open (database.encode("utf-8"))
		if self._hConn == None:
			print ("Can't open connection")

	def __del__(self):
		self.close ()

	def cursor(self):
		if self._hConn == None:
			return None
		return CrossDbCursor(self._hConn)

	def close(self):
		if self._hConn != None:
			CrossDbInterface.libCrossdb.xdb_close (self._hConn)

	def commit(self):
		CrossDbInterface.libCrossdb.xdb_commit (self._hConn)

	def rollback(self):
		CrossDbInterface.libCrossdb.xdb_rollback (self._hConn)

class CrossDbCursor(object):
	def __init__(self, hConn):
		self._hConn = hConn
		self._libCrossdb = CrossDbInterface.libCrossdb
		self._hResult = None

	def __del__(self):
		self.close()

	def __iter__(self):
		return self

	def next(self):
		return self.__next__()

	def __next__(self):
		row = self.fetchone()
		if None == row:
			raise StopIteration
		return row

	@property
	def rowcount(self):
		return self._rowCount

	@property
	def affected_rows(self):
		return self._affectedRows

	@property
	def description(self):
		self._description = []
		if self._rowMeta > 0:
			pColList = ctypes.cast(self._rowMeta + 16, ctypes.POINTER(ctypes.c_uint64))[0]
			for i in range (self._fldCount):
				pCol = ctypes.cast(pColList, ctypes.POINTER(ctypes.c_uint64))[i]
				bytes = ctypes.cast(pCol + 10, ctypes.POINTER(ctypes.c_uint8))[0]
				colName = ctypes.cast(pCol + 11, ctypes.POINTER(ctypes.c_char*bytes))[0].value
				colType = ctypes.cast(pCol + 2, ctypes.POINTER(ctypes.c_int8))[0]
				self._description.append ((colName, colType, None, None, None, None, False))		
		return self._description

	def close(self):
		if self._hResult != None:
			self._libCrossdb.xdb_free_result(self._hResult)
			self._hResult = None

	def execute(self, sql):
		if self._hResult != None:
			self._libCrossdb.xdb_free_result(self._hResult)
		self._hResult = self._libCrossdb.xdb_exec (self._hConn, sql.encode("utf-8"))
		self._errno = ctypes.cast(self._hResult + 4, ctypes.POINTER(ctypes.c_uint16))[0]
		self._rowMeta = ctypes.cast(self._hResult + 5*8, ctypes.POINTER(ctypes.c_uint64))[0]
		self._rowCount = ctypes.cast(self._hResult + 2*8, ctypes.POINTER(ctypes.c_uint64))[0]
		self._fldCount = ctypes.cast(self._hResult + 12, ctypes.POINTER(ctypes.c_uint16))[0]
		self._affectedRows = ctypes.cast(self._hResult + 3*8, ctypes.POINTER(ctypes.c_uint64))[0]
		if self._rowMeta > 0:
			pColList = ctypes.cast(self._rowMeta + 16, ctypes.POINTER(ctypes.c_uint64))[0]
			self._fieldType = []
			for i in range (self._fldCount):
				pCol = ctypes.cast(pColList, ctypes.POINTER(ctypes.c_uint64))[i]
				colType = ctypes.cast(pCol + 2, ctypes.POINTER(ctypes.c_int8))[0]
				self._fieldType.append (colType)
		else:
			self._fieldType = None
			self._hResult = None
		return self._errno

	def fetchone(self):
		row = []
		xrow = self._libCrossdb.xdb_fetch_row(self._hResult)
		if None == xrow:
			return None
		for i in range (self._fldCount):
			field = ctypes.cast(xrow, ctypes.POINTER(ctypes.c_uint64))[i]
			if None != field:
				type = self._fieldType[i]
				if CrossDbType.XDB_TYPE_INT == type:
					value = ctypes.cast(field, ctypes.POINTER(ctypes.c_int32))[0]
					row.append (value)
				elif CrossDbType.XDB_TYPE_CHAR == type:
					bytes = ctypes.cast(field-2, ctypes.POINTER(ctypes.c_uint16))[0]
					value = ctypes.cast(field, ctypes.POINTER(ctypes.c_char*bytes))[0].value
					row.append (value)
				elif CrossDbType.XDB_TYPE_SMALLINT == type:
					value = ctypes.cast(field, ctypes.POINTER(ctypes.c_int16))[0]
					row.append (value)
				elif CrossDbType.XDB_TYPE_TINYINT == type:
					value = ctypes.cast(field, ctypes.POINTER(ctypes.c_int8))[0]
					row.append (value)
				elif CrossDbType.XDB_TYPE_FLOAT == type:
					value = ctypes.cast(field, ctypes.POINTER(ctypes.c_float))[0]
					row.append (value)
				elif CrossDbType.XDB_TYPE_DOUBLE == type:
					value = ctypes.cast(field, ctypes.POINTER(ctypes.c_double))[0]
					row.append (value)
				else:
					print ("Unknow type: "+str(self._fieldType[i]))
			else:
				row.append (None)
		return row

	def fetchall(self):
		rows=[]
		while True:
			row = self.fetchone ()
			if None == row:
				break;
			rows.append (row)
		return rows

	def executemany(self, operation, seq_of_parameters):
		pass

	def fetchmany(self):
		pass

	def commit(self):
		self._libCrossdb.xdb_commit (self._hConn)

	def rollback(self):
		self._libCrossdb.xdb_rollback (self._hConn)

	def callproc(self, procname, *args):
		pass

if __name__ == '__main__':

	conn = connect(database=":memory:")
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
