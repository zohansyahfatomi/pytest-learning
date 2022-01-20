#!/usr/bin/python
# -*- coding: utf-8 -*-
from pymysql import connect, Error


class Databases(object):

    def __init__(self, kata):
        self.kata = kata

   # SELECT data

    def select_db():
        try:
            global conn, cursor
            conn = connect(host='localhost', user='root', password='',
                           db='db_kafka')

            cursor = conn.cursor()

            sql_select_query = 'SELECT kata from Msg'
            cursor.execute(sql_select_query)
            rs = cursor.fetchall()
            return rs

        except Error, error:

            print 'Failed to select record to database rollback: {}'.format(error)
            conn.rollback()
        finally:

            cursor.close()
            conn.close()

   # INSERT data

    def insert_db():
        try:
            global conn, cursor
            conn = connect(host='localhost', user='root', password='',
                           db='db_kafka')

            cursor = conn.cursor()

            kata = 'ini kata'
            sql_insert_query = 'INSERT INTO Msg (kata) VALUES (%s)'
            values = kata
            cursor.execute(sql_insert_query, values)
            conn.commit()
        except Error, error:

            print 'Failed to insert record to database rollback: {}'.format(error)
            conn.rollback()
        finally:

            cursor.close()
            conn.close()

   # UPDATE data

    def update_db():
        try:
            global conn, cursor
            conn = connect(host='localhost', user='root', password='',
                           db='db_kafka')

            cursor = conn.cursor()

            kata = 'ini kata'
            kata_ubah = 'ini kata ubah'
            sql_insert_query = \
                'UPDATE  Msg SET kata = %s WHERE kata = %s'
            values = (kata_ubah, kata)
            cursor.execute(sql_insert_query, values)
            conn.commit()
        except Error, error:

            print 'Failed to update record to database rollback: {}'.format(error)
            conn.rollback()
        finally:

            cursor.close()
            conn.close()
