import contextlib
import sqlite3
import pandas as pd
import mysql.connector as mysqld
import time
import logging


class Sqlite3db(object):
   def __init__(self, v_db_path):
      self.__Conn = sqlite3.connect(v_db_path, check_same_thread=False)
      self.__Retries = 10

   def execute_sql(self, v_query, v_values=None):
      o_result = None

      with contextlib.closing(self.__Conn.cursor()) as o_cursor:
          b_Clt = False
          i_counter = 0

          while i_counter < self.__Retries and not b_Clt:
             i_counter += 1

             try:
                if v_values is None :
                    o_cursor.execute(v_query)
                else :
                    o_cursor.execute(v_query, v_values)
                self.__Conn.commit()
                b_Clt = True
             except Exception as e:
                raise (e) 
              
          o_result = o_cursor.fetchall()

      return o_result

   def execute_pd(self, v_sql, v_values=None):
      o_result = None

      if v_values is None:
         o_result = pd.read_sql_query(v_sql, self.__Conn)
      else:
         o_result = pd.read_sql_query(v_sql, self.__Conn, params=v_values)


      self.disconnect()


      return o_result

   def disconnect(self):
      if self.__Conn is not None :  self.__Conn.close()


class MySQLdb:
    def __init__(self, ip, port, username, password, auto=True):
        self.db = None

        try:
            self.db = mysqld.connect(
                host=ip,
                port=port,
                user=username,
                passwd=password,
                connection_timeout=10,
                autocommit=auto
            )
        except mysqld.Error as err:
            logging.exception(err)

    def is_connected(self):
        
        b_ret = False

        if self.db != None:
            b_ret = self.db.is_connected()

        return b_ret

    def execute_pd_query(self, sql):
        cursor = self.db.cursor()
        init_time = time.time()
        cursor.execute(sql)
        after_time = time.time()
        res = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        cursor.close()

        return res, (round(after_time - init_time,1))

    def execute_ddl_query(self, sql):
        cursor = self.db.cursor()
        init_time = time.time()
        cursor.execute(sql)
        after_time = time.time()
        cursor.close()

        return (round(after_time - init_time,1))

    def execute_query(self, sql):
        #self.db.autocommit = False
        cursor = self.db.cursor(buffered=True)
        init_time = time.time()
        cursor.execute(sql, multi=True)
        cursor.close()
        after_time = time.time()

        return (round(after_time - init_time,1))

    def execute_callproc(self, proc, args):
        o_out = ''
        o_resp = None

        cursor = self.db.cursor(buffered=True)
        init_time = time.time()
        o_resp = cursor.callproc(proc, args)
        after_time = time.time()
        for result in cursor.stored_results():
            for row in result.fetchall():
                o_out += ''.join(row)
                o_out += "\n"
        cursor.close()

        return o_out, o_resp, (round(after_time - init_time,1))

    def disconnect(self):
        if self.db is not None:
            self.db.close()    
