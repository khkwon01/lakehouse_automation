#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is generater of data regarding airport passenger survey
"""
__title__ = 'datagen'
__version__ = '1.0.0-DEV'
__original_author__ = 'khkwon01'
__modified_author__ = 'khkwon01'
__license__ = 'MIT'
__copyright__ = 'Copyright 2024'

import os
import schedule, sys
import random, time, csv
import logging, argparse
from datetime import datetime
from dataclasses import dataclass
from pytz import timezone

mountpoint="/root/lakehouse"
nid = range(1,129880)
customer_type = ["First-time", "Returning"]
travel_type = ["Personal", "Business"]
dep_delay = range(0, 1600)
baggage_handling = range(1, 5)
satisfaction = ["Neutral or Dissatisfied", "Satisfied"]

@dataclass
class Passurvey():
    id: int
    custype: str
    tratype: str
    delay: int
    baghand: int
    satisf: str

def get_args_parser():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-m", "--mountloc",
                        type=str, action="store",
                        default=mountpoint, 
                        help="mount location for storing generated data")
    parser.add_argument("-r", "--rowcount",
                        type=int, action="store",
                        default=10000,
                        help="row count for generating data")
    parser.add_argument("-d", "--delete",
                        type=int, action="store",
                        default=1,
                        help="days required for deletion")
    parser.add_argument("--debug",
                        default=True,
                        action='store_true',
                        help="Debug log enable.")
    parser.add_argument("--help",
                        default=False,
                        action='store_true',
                        help="show this help message and exit.")
    return parser

def make_data(s_mountloc, i_rowcount):
    logging.info(f"making data... param : {s_mountloc},{i_rowcount}")

    c_date = datetime.now(timezone('Asia/Seoul')).strftime("%Y%m%d%H%M")   
    s_csv_file = f"{mountpoint}/datagen_{c_date}.csv"

    try:
        csvfile = open(s_csv_file, 'w', newline='\n')
        csvwriter = csv.writer(csvfile, delimiter=',', lineterminator='\n')
        csvwriter.writerow(['id','customer_type','travel_type', \
                          'departure_delay','baggage_handling','satisfaction'])

        for count in range(i_rowcount):
           id = random.choice(nid)
           ct = random.choice(customer_type)
           tt = random.choice(travel_type)
           dd = random.choice(dep_delay)
           bh = random.choice(baggage_handling)
           sf = random.choice(satisfaction)

           csvwriter.writerow([id, ct, tt, dd, bh, sf])

    except Exception as e:
        logging.exception(f"Error making data\n{e}")
    finally:
        csvfile.close()

    logging.info(f"completed making data...")

def delete_data(i_delday):
    logging.info("deleting data... param : {i_delday}")

    o_filelist = os.listdir(mountpoint)

    current_ts = (int(time.time()) - 24*60*60*int(i_delday))

    for o_file in o_filelist:
        mtime = os.path.getmtime(f"{mountpoint}/{o_file}")

        if ( mtime < current_ts ):
            logging.info(f"deleted file : {o_file}")
            os.remove(f"{mountpoint}/{o_file}")

    logging.info(f"completed deleting data...")

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
            filename="logs/datagen.log",
            filemode='w',
        )
        logging.debug(options)
    else:
        nl_hanlder = logging.NullHandler(logging.INFO)
        logging.basicConfig(handlers = [ nl_hanlder ])

    job1 = schedule.every(1).minutes.do(make_data, options.mountloc, options.rowcount)
    #job1 = schedule.every(10).seconds.do(make_data, options.mountloc, options.rowcount)
    job2 = schedule.every(1).hour.do(delete_data, options.delete)

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt as ke:
            print("datagen was stopped by ctrl+c")
            logging.info(f"datagen was stopped by ctrl+c\n\t{ke}")
            sys.exit()
        except Exception as e:
            print("datagen was stopped")
            logging.info(f"datagen was stopped\n\t{e}")
            sys.exit()
