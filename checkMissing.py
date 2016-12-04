#coding:utf-8

import csv
import time
import datetime
import talib
import pandas
import numpy as np

from DBHelper import DBHelper
from DBConstants import DBConstants

READ_DB_PATH = "/home/k-sui/fxdata/fxdata.db"

# 各インジケータを近いレンジになるように整形する()
def checkMissing():

    # 読み込み用のDBHelperを作成(on memory)
    readIndiDBHelper = DBHelper(READ_DB_PATH, DBConstants.INDICATOR_1HOUR_TBL, DBConstants.INDICATOR_COLUMNS)

    prev = None

    for row in readIndiDBHelper.getAllDataCursor():
        now = datetime.datetime.strptime(row[0], DBConstants.TIME_FORMAT)
        if prev == None:
            prev = now
            continue

        delta = now - prev
        if delta != datetime.timedelta(hours=1):
            print (prev.strftime(DBConstants.TIME_FORMAT) + " --- " + row[0])

        prev = now

if __name__ == "__main__":
    checkMissing()

