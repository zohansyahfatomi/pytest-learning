from pymysql import connect, Error
from sqlalchemy import insert

def select_db():
   try:
      global conn, cursor
      conn = connect(
         host="localhost",
         user="root",
         password="",
         db="db_kafka"
         )

      cursor = conn.cursor()

      #executing sql
      #table parameter!
      #kolom parameter!
      #bikin insert dan update!
      
      sql_select_query = "SELECT kata from Msg"
      cursor.execute(sql_select_query)
      rs = cursor.fetchall()
      print(rs)

   except Error as error:
      print("Failed to select record to database rollback: {}".format(error))
      conn.rollback()

   finally:
      cursor.close()
      conn.close()

def insert_db():
   try:
      global conn, cursor
      conn = connect(
         host="localhost",
         user="root",
         password="",
         db="db_kafka"
         )

      cursor = conn.cursor()

      #executing sql
      #table parameter!
      #kolom parameter!
      #bikin insert dan update!
      
      kata = "ini kata"
      sql_insert_query = "INSERT INTO Msg (kata) VALUES (%s)"
      values = (kata)
      cursor.execute(sql_insert_query, values)
      conn.commit()      

   except Error as error:
      print("Failed to select record to database rollback: {}".format(error))
      conn.rollback()

   finally:
      cursor.close()
      conn.close()

#insert_db()
select_db()