#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is checking changed data in object storage and update lakehouse
"""
__title__ = 'check_lakehouse'
__version__ = '1.0.0-DEV'
__original_author__ = 'khkwon01'
__modified_author__ = 'khkwon01'
__license__ = 'MIT'
__copyright__ = 'Copyright 2024'

import os
import argparse, getpass
import logging, sqlite3, time
import threading, json
import traceback, signal
from datetime import datetime
from pytz import timezone

import mysql.connector as mysql
from lib.commondb import Sqlite3db, MySQLdb

mountpoint="/root/lakehouse"


def get_args_parser():
    parser = argparse.ArgumentParser(add_help=False)

    parser.add_argument("-m", "--mountloc",
                        type=str, action="store",
                        default=mountpoint, nargs="?",
                        help="mount location for storing generated data")    

    parser.add_argument("-h", "--host",
                        type=str, action="store",
                        default="127.0.0.1", nargs="?",
                        help="heatwave database ip address.")

    parser.add_argument("-P", "--port", 
                        type=int, action="store",
                        default=3306, nargs="?",
                        help="heatwave database port number.")

    parser.add_argument("-d", "--database",
                        type=str, action="store",
                        default="test", nargs="?",
                        help="heatwave database")

    parser.add_argument("-u", "--user",
                        type=str, action="store",
                        default="admin", nargs="?", 
                        help="database user")

    parser.add_argument("-p", "--password",
                        type=str, action="store",
                        default="Welcome#1", nargs="?",
                        help="database user password")

    parser.add_argument("-r", "--repodb",
                        type=str, action="store",
                        default="lake.db", nargs="?",
                        help="sqlitedb(repo) name")

    parser.add_argument("--debug",
                        default=True,
                        action='store_true',
                        help="Debug log enable.")

    parser.add_argument("--help",
                        default=False,
                        action='store_true',
                        help="show this help message and exit.")

    return parser

class UpdateLakehouse(threading.Thread):
    
    _stop = False
    _interval = 600
    _params = None
    _repodb = None
    _mysqldb = None

    def __init__(self, **kwargs):

        self._params = kwargs.get("params")
        self.lock = threading.Lock()
        threading.Thread.__init__(self, name="UpdateLakehouse")
        self.setDaemon(True)

        self._repodb = Sqlite3db(self._params.repodb)
        self._mysqldb = MySQLdb(self._params.host, self._params.port, 
                                self._params.user, self._params.password)

    def run(self):

        try:
            s_repo_select1="select checktime from check_files order by checktime desc limit 2";
            s_repo_insert1="insert into check_files (mountloc, is_apply) values(?,?)"

            while self._stop == False:
                logging.info("looping....")
                o_ret = self._repodb.execute_sql(s_repo_select1)
                files = os.listdir(self._params.mountloc)
                latest_file = max(files)

                mtime = os.path.getmtime(f"{self._params.mountloc}/{latest_file}")
                check_ts = (int(time.time()) - int(self._interval))

                if ( mtime >= check_ts ):
                    o_ret = self.add_incremental_data()

                    if o_ret.find("Table load succeeded!") > 0:
                        o_ret = self._repodb.execute_sql(s_repo_insert1, \
                                (self._params.mountloc, 1))
                    else:
                        o_ret = self._repodb.execute_sql(s_repo_insert1, \
                                (self._params.mountloc, 0))
                else:
                    logging.info("There was no changed files...")

                time.sleep(self._interval)


        except KeyboardInterrupt as ke:
            logging.exception("Err :" + str(e))
        except Exception as e:
            s_trackmsg = traceback.format_exc()
            logging.exception("Err :" + str(e) + "\n" + s_trackmsg) 

    def signal(self, signum, frame):
        logging.info("The UpdateLakehouse is stopping.")
        self._intdb.disconnect()
        self._mysqldb.disconnect()
        self._stop = True

    def add_incremental_data(self):
        logging.info("call add_incremental_data.....")

        if self._mysqldb.is_connected() == False:
            self._mysqldb = MySQLdb(self._params.host, self._params.port,
                                self._params.user, self._params.password)
            logging.info("reconnect to MySQL db")
        else:
            dbs = f'["{self._params.database}"]'
            options = {"mode": "normal", "refresh_external_tables": True}

            o_out, _, exec_time = self._mysqldb.execute_callproc('sys.heatwave_load',
                                   (dbs, json.dumps(options)))

            c_date = datetime.now(timezone('Asia/Seoul')).strftime("%Y%m%d%H%M")
            s_out_file = f"logs/refresh_result_{c_date}.log"

            ofile = open(s_out_file, "w")
            ofile.write(o_out)
            ofile.close()

        return o_out


if __name__ == '__main__':
    parser = get_args_parser()
    options = parser.parse_args()

    if options.help:
        parser.print_help()
        parser.exit()

    if options.debug:
        if not os.path.isdir("logs"):
            os.mkdir("logs")
        logging.basicConfig(
            format='%(asctime)s - (%(threadName)s) - %(message)s in %(funcName)s() at %(filename)s : %(lineno)s',
            level=logging.DEBUG,
            filename="logs/check_lakehouse.log",
            filemode='w',
        )
        logging.debug(options)
    else:
        nl_hanlder = logging.NullHandler(logging.INFO)
        logging.basicConfig(handlers = [ nl_hanlder ])

    lakemon = UpdateLakehouse(params=options)
    signal.signal(signal.SIGTERM, lakemon.signal)
    lakemon.start()
    lakemon.join()
