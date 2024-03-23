import pandas as pd
#from sqlalchemy import create_engine
import psycopg2
import sys
from io import StringIO

class postgresConn:
    def __init__(self, params_dic):
        self.para=  params_dic
        """ Connect to the PostgreSQL database server """
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(**params_dic)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            sys.exit(1) 
        print("Connection successful")
        
    def Getcursor(self):
        #create a connection to DB
        cursor = self.conn.cursor()
        return cursor
    
    def GetConnection(self):
        #create a connection to DB
        conn = self.conn
        return conn
    
    def ExecuteSQL(self, sqlQuery):

        #create a connection to DB
        cursor = self.conn.cursor()
        try:
            cursor.execute(sqlQuery)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            #os.remove(tmp_df)
            message = "Error: %s" % error
            print(message)
            self.conn.rollback()
            cursor.close()
            return message
        print("SqlQuery ( {0} ) on the Database have been completed".format(sqlQuery))

        cursor.close()

        return "data writing have been completed with Upsert method"


        #create a connection to DB

        #create a memmory buffor to hold data
        buffer = StringIO()

        #load data into the buffer
        df.to_csv(buffer, index_label=False, header=False, index=False, sep='|')

        cursor = self.conn.cursor()
        buffer.seek(0)
        
        
        sql = "delete from " + stgTable
        sql1 = "delete from " + table
        ##Query1=  "delete from {0}  a using  {1} b where a.{2} = b.{2}".format( table, stgTable,  keycol )


        try:
            cursor.execute(sql)

            cursor.copy_from(buffer, stgTable, sep="|")
            #self.conn.commit()

            cursor.execute(sql1)

            # delete from main table based on new record
            #cursor.execute(Query1)
            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            #os.remove(tmp_df)
            message = "Error: %s" % error
            print(message)
            self.conn.rollback()
            cursor.close()
            return message
        print("Data load to Stagging table have been Completed")

        cols = ','.join(list(df.columns))
        columns = list(df.columns)
        #columns.remove(keycol)
        updatecol  = { a: 'EXCLUDED.'+a for a  in columns }
        str1= ''
        for i, k in updatecol.items():
            if len(str1)>0:
                str1 = str1+','+ i+'='+k 
            else:
                str1=  i+'='+k 
        updatecol = str1

        query2  = "INSERT INTO {0}({1}) SELECT * FROM {2} ".format(table, cols, stgTable)

        print(query2)
        cursor.execute(query2)
        
        self.conn.commit()
        cursor.close()

        return "data writing have been completed with full load method"