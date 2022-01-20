from pymysql import connect, Error

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
      
      kata = "ini kata"
      sql_insert_query = "INSERT INTO Msg (kata) VALUES (%s)"
      values = (kata)
      cursor.execute(sql_insert_query, values)
      conn.commit()      

   except Error as error:
      print("Failed to insert record to database rollback: {}".format(error))
      conn.rollback()

   finally:
      cursor.close()
      conn.close()
      
def update_db():
   try:
      global conn, cursor
      conn = connect(
         host="localhost",
         user="root",
         password="",
         db="db_kafka"
         )

      cursor = conn.cursor()
      
      kata = "ini kata"
      kata_ubah = "ini kata ubah"
      sql_insert_query = "UPDATE  Msg SET kata = %s WHERE kata = %s"
      values = (kata_ubah, kata)
      cursor.execute(sql_insert_query, values)
      conn.commit()      

   except Error as error:
      print("Failed to update record to database rollback: {}".format(error))
      conn.rollback()

   finally:
      cursor.close()
      conn.close()

