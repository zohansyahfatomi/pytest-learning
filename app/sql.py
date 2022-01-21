#!/usr/bin/python
# -*- coding: utf-8 -*-
from pymysql import connect, Error, cursors


class Databases(object):

    def __init__(self):
        pass 

   # SELECT data

    def select_db(self,new_data):
        try:

            global conn, cursor
            conn = connect(host="localhost", user="root", password="",
                           db="db_kafka", cursorclass = cursors.DictCursor)

            cursor = conn.cursor()

            #tambahin kolom
            sql_select_query = "SELECT kata from msg where id = %s"
            values = new_data
            cursor.execute(sql_select_query, values)
            rs = cursor.fetchone()
            return rs
            

        except Error as error:

            print ("Failed to select record to database rollback: {}".format(error))
            conn.rollback()
        finally:

            cursor.close()
            conn.close()

   # INSERT data

    def insert_db(self, new_data):
        try:
            global conn, cursor
            conn = connect(host='localhost', user='root', password='',
                           db='db_kafka')

            cursor = conn.cursor()

            sql_insert_query = 'INSERT INTO msg (kata) VALUES  (%s)'
            values = new_data
            cursor.execute(sql_insert_query, values)
            conn.commit()
            return True

        except Error as error:
            print ('Failed to insert record to database rollback: {}'.format(error))
            conn.rollback()
        finally:

            cursor.close()
            conn.close()

   # UPDATE data

    def update_db(self, old_data, new_data):
        try:
            global conn, cursor
            conn = connect(host='localhost', user='root', password='',
                           db='db_kafka')

            cursor = conn.cursor()
    
            sql_insert_query = \
                'UPDATE  msg SET kata = %s WHERE kata = %s'
            values = (new_data, old_data)
            cursor.execute(sql_insert_query, values)
            conn.commit()
            return True

        except Error as error:
            print ('Failed to update record to database rollback: {}'.format(error))
            conn.rollback()
        finally:

            cursor.close()
            conn.close()
