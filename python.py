from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
import os
from datetime import datetime
import requests
import json
import csv
import pandas as pd
import pyodbc as  odbc
import os
import csv

pathCSV_1 = 'N:\py_scripts\IR\drivers.csv'
Table = 'tmp'
db = 'Sales_Force'

args = {
    'owner': 'max',
    'start_date':datetime.datetime(2022, 8, 10),
    'provide_context':True
}


class SQL():
	"""docstring for SQL"""
	def __init__(self, db,table=''):
		super(SQL, self).__init__()
		self.db = db
		self.table = table
		DRIVER = 'odbc driver 17 for sql server'
		SERVER_NAME = 'WIPRD267'   
		conn_string = f"""
	        Driver={{{DRIVER}}};
	        Server={SERVER_NAME};
	        Database={self.db};
	        Trusted_Connection=yes;
	    """
	    #Connceting to SQL OLEDB
		try:
			self.conn = odbc.connect(conn_string)
			#self.conn.setencoding(encoding='cp1251')
			self.cursor = self.conn.cursor()
		except Exception as e:
			#print(e)
			pass

	def bulk(self,path,delimiter=",",row_separator="\r\n",start_from_row="1",ignore_codepage = '',code_page = '65001',custom='--'):
		query= f"""
		    BULK INSERT {self.table}
            FROM '{path}'
            
            WITH (FIRSTROW = {start_from_row},
           	{ignore_codepage}CODEPAGE = '{code_page}',
            FIELDTERMINATOR = '{delimiter}',
            {custom},
            ROWTERMINATOR='{row_separator}',
            KEEPNULLS,
            FORMAT ='CSV',
            MAXERRORS = 9999999 )
		"""
		self.cursor.execute(query)
		self.cursor.commit()

	def Query(self,cols=[],sql_query="",header = True):
		# SELECT all as default
		if len(cols)>0:
			tmp_cols = cols[:]
			cols = str.join(', ',cols)
		else:
			cols = "*"
		if sql_query == "":
			sql_query =f"Select {cols} from {self.table}"
			#print(sql_query)
		# Running query
		#print(sql_query)
		self.cursor.execute(sql_query)
		if sql_query.lower().find('select') < 0:
			#print(sql_query.lower().find('select'))
			self.cursor.commit()
		#print("!")
		try:
			tmp_cols = [column[0] for column in self.cursor.description]
		except:
			pass
		array = []
		try:
			for row in self.cursor.fetchall():
				#print(row)
				array.append(list(row))
			if header is True:
				array.insert(0,tmp_cols)
				return array
			else:
				return array
		except Exception as e:
			#print(e)
			pass

def getData():

	response = requests.get("https://random-data-api.com/api/cannabis/random_cannabis?size=10")
	df = pd.DataFrame(json.loads(response.content))

	df.to_csv(pathCSV_1, sep=',' ,escapechar='\\', encoding='utf-8')
	#print(df.info())

	return df

def loadData(df):

	headers = list(df)

	for i,column in enumerate(df):
		headers[i]= f'[{column}] [varchar](255) NULL'
	headers = str.join(', ', headers)

	sql_query_create = f'''
	if  object_id('{Table}') is not null  drop table {Table}
	; 
	CREATE TABLE {Table} (id1 [varchar](255) NULL, {headers})'''

	sqlTableIn = SQL(db,Table)
	sqlTableIn.Query(sql_query=sql_query_create)

	sqlTableIn.bulk(pathCSV_1,start_from_row = '2')
	os.remove(pathCSV_1)

with DAG('aero_task', description='aero_task', schedule_interval= timedelta(hours=12),  catchup=False,default_args=args) as dag:
        getData    = PythonOperator(task_id='getData', python_callable=getData)
        loadData  = PythonOperator(task_id='loadData', python_callable=loadData)

        getData >> loadData 