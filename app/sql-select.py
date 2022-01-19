import pymysql

def select_db():
   try:
      global conn, cursor
      conn = pymysql.connect(
         host="localhost",
         user="root",
         password="",
         db="db_kafka"
         )

      cursor = conn.cursor()

      #executing sql
      sql_select_query = 'SELECT kata from Msg'
      cursor.execute(sql_select_query)
      rs = cursor.fetchall()
      print(rs)
      print("connection is sucess! ")

   except pymysql.Error as error:
      print("Failed to update record to database rollback: {}".format(error))
      conn.rollback()

   finally:
      cursor.close()
      conn.close()

select_db()
 