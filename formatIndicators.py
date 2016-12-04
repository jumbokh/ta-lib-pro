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
WRITE_DB_PATH = "/home/k-sui/fxdata/fxdata.db"

# 各インジケータを近いレンジになるように整形する()
def formatIndicators():

    # 読み込み用のDBHelperを作成(on memory)
    readIndiDBHelper = extractToMemory(READ_DB_PATH, DBConstants.INDICATOR_1HOUR_TBL, DBConstants.INDICATOR_COLUMNS, DBConstants.CREATE_INDICATOR_TABLE_STRING)
    readChartDBHelper = extractToMemory(READ_DB_PATH, DBConstants.CHART_1HOUR_TBL, DBConstants.CHART_COLUMNS, DBConstants.CREATE_CHART_TABLE_STRING)

    # 書き込み用のDBHelperを作成
    writeDBHelper =  DBHelper(WRITE_DB_PATH, DBConstants.INDICATOR_1HOUR_FORMAT_TBL, DBConstants.INDICATOR_COLUMNS)

    # 整形後のデータを格納するテーブルを作成
#    writeDBHelper.createTable(DBConstants.CREATE_INDICATOR_TABLE_STRING)

    for row in readIndiDBHelper.getAllDataCursor():
    # row = [time, sma5, sma25, rsi9, rsi11, rsi14, macd, macdsignal, macdhist, cci14, cci20, stocSlowk, stocSlowd, stocFastk, stocFastd, stocRSIk, stocRSId, trima5, trima25]

        # 同時刻の終値を取得
        closePrice = readChartDBHelper.getCursor(row[0]).fetchone()[4]

        # 各カラムをそれぞれ整形
        formated = [row[0], \
                    formatMA(row[1],closePrice), formatMA(row[2],closePrice), \
                    formatRSI(row[3]), formatRSI(row[4]), formatRSI(row[5]), \
                    formatMACD(row[6]), formatMACD(row[7]), formatMACD(row[8]), \
                    formatCCI(row[9]), formatCCI(row[10]), \
                    formatSTOC(row[11]), formatSTOC(row[12]), formatSTOC(row[13]), formatSTOC(row[14]), formatSTOC(row[15]), formatSTOC(row[16]), \
                    formatTRIMA(row[17],closePrice), formatTRIMA(row[18],closePrice)]

        # debug
#        print("compare row with formated")
#        print(row)
#        print(formated)
        # 書き込み
        writeDBHelper.add(formated)

    writeDBHelper.commit()

# 移動平均からの終値の乖離率のイメージ。-1〜1のレンジにはかなり小さい値になるので、10倍しておく。
def formatMA(ma, close):
    if ma == 'nan':
        return 'nan'
    return (close-ma)*100.0 / ma

# RSIは値が0〜100%なので、レンジが-1〜1になるよう、50を引いて100で割る
def formatRSI(rsi):
    if rsi == 'nan':
        return 'nan'
    return (rsi * 1.0 - 50) / 100

# MACDはとりあえずそのままでレンジが合いそう
def formatMACD(macd):
    if macd == 'nan':
        return 'nan'
    return macd

# ±100%の前後で判断するので100で割ればおおよそレンジは合いそう
def formatCCI(cci):
    if cci == 'nan':
        return 'nan'
    return cci/100

# ストキャスティクスは値が0〜100%なので、レンジが-1〜1になるよう、50を引いて100で割る
def formatSTOC(stoc):
    if stoc == 'nan':
        return 'nan'
    return (stoc * 1.0 - 50) / 100

# 三角移動平均のレンジは移動平均と同じなので同じ計算式
def formatTRIMA(trima, close):
    if trima == 'nan':
        return 'nan'
    return (close-trima)*10.0 / trima


def extractToMemory(dbPath, tableName, columns, createTBString):

    orgDB = DBHelper(dbPath, tableName, columns)

    onMemoryDB = DBHelper(':memory:', tableName, columns)
    onMemoryDB.createTable(createTBString)

    for row in orgDB.getAllDataCursor():
        onMemoryDB.add(row)

    orgDB.close()
    orgDB = None

    return onMemoryDB

if __name__ == "__main__":
    formatIndicators()