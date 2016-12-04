#coding:utf-8

import csv
import time
import datetime
import talib
import pandas
import numpy as np

from DBHelper import DBHelper
from DBConstants import DBConstants

DB_PATH = "/home/k-sui/fxdata/fxdata.db"

#1分足のチャートからX分足のチャートを作成する
#TODO 生成できなかった時刻がある場合はそれを警告
def add2DB():


    # X分足の読み込むためのDBHelper
    chartXminHelper = createChartXminHelper(DB_PATH, 60)

    # 読み込み用のDBを開く。priceに全データを詰め込む。[open, high, low, close]
    prices = np.empty((0,4), float)
    time = []
    for row in chartXminHelper.getAllDataCursor().fetchall():
        # float型に変換してnumpy配列に挿入
        tmp = np.array([[float(row[i]) for i in range(1,5)]])
        prices = np.append(prices, tmp, axis=0)
        # 時間は別に保持
        time.append(row[0])

    # 使い終わったので解放
    chartXminHelper = None

    # X分足のインジケータを書き込むためのDBHelper
    indicatorXminHelper = createIndicatorXminHelper(DB_PATH, 60)

    # SMA
    sma5 = talib.SMA(prices[:, 3], timeperiod=5)    #SMA(close, timeperiod=5)
#    sma5 = talib.SMA(prices[:, 4], timeperiod=5)    #SMA(close, timeperiod=5)
    sma25 = talib.SMA(prices[:, 3], timeperiod=25)

    # RSI
    rsi9 = talib.RSI(prices[:, 3], timeperiod=9)    #RSI(close, timeperiod=9)
    rsi11 = talib.RSI(prices[:, 3], timeperiod=11)
    rsi14 = talib.RSI(prices[:, 3], timeperiod=14)

    # MACD
    macd, macdsignal, macdhist = talib.MACD(prices[:, 3], fastperiod=12, slowperiod=26, signalperiod=9) #MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)

    # CCI
    cci14 = talib.CCI(prices[:, 1], prices[:, 2], prices[:, 3], timeperiod=14)  #CCI(high, low, close, timeperiod=14)
    cci20 = talib.CCI(prices[:, 1], prices[:, 2], prices[:, 3], timeperiod=20)

    # Stochastic
    stocSlowk, stocSlowd = talib.STOCH(prices[:, 1], prices[:, 2], prices[:, 3], fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3,slowd_matype=0)     # STOCH(high, low, close, fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
    # Stochastic Fast
    stocFastk, stocFastd = talib.STOCHF(prices[:, 1], prices[:, 2], prices[:, 3], fastk_period=5, fastd_period=3, fastd_matype=0)
    # Stochastic Relative Strength Index
    stocRSIk, stocRSId = talib.STOCHRSI(prices[:, 3], timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)

    # 三角移動平均。移動平均と合わせて期間は5,25で
    trima5 = talib.TRIMA(prices[:, 3], timeperiod=5)     # TRIMA(close, timeperiod=30)
    trima25 = talib.TRIMA(prices[:, 3], timeperiod=25)  # TRIMA(close, timeperiod=30)

    for i in range(len(prices) - 1):

        # DBへの書き込み
        indicatorXminHelper.add([time[i], \
                                 sma5[i], sma25[i], \
                                 rsi9[i], rsi11[i], rsi14[i], \
                                 macd[i], macdsignal[i], macdhist[i], \
                                 cci14[i], cci20[i], \
                                 stocSlowk[i], stocSlowd[i], \
                                 stocFastk[i], stocFastd[i], \
                                 stocRSIk[i], stocRSId[i], \
                                 trima5[i], trima25[i] \
                                 ])

        if i % 1000 == 0:
            print(time[i])
            indicatorXminHelper.commit()


# 指定された時間間隔用のDBHelperを作成
def createChartXminHelper(dbPass, Xmin):

    if Xmin == 1:
        chartXminHelper = DBHelper(dbPass, DBConstants.CHART_1MIN_TBL, DBConstants.CHART_COLUMNS)
        return chartXminHelper

    elif Xmin == 5:
        chartXminHelper = DBHelper(dbPass, DBConstants.CHART_5MIN_TBL, DBConstants.CHART_COLUMNS)
        return chartXminHelper

    elif Xmin == 10:
        chartXminHelper = DBHelper(dbPass, DBConstants.CHART_10MIN_TBL, DBConstants.CHART_COLUMNS)
        return chartXminHelper

    elif Xmin == 30:
        chartXminHelper = DBHelper(dbPass, DBConstants.CHART_30MIN_TBL, DBConstants.CHART_COLUMNS)
        return chartXminHelper

    elif Xmin == 60:
        chartXminHelper = DBHelper(dbPass, DBConstants.CHART_1HOUR_TBL, DBConstants.CHART_COLUMNS)
        return chartXminHelper

    elif Xmin == 240:
        chartXminHelper = DBHelper(dbPass, DBConstants.CHART_4HOUR_TBL, DBConstants.CHART_COLUMNS)
        indicatorXminHelper = DBHelper(dbPass, DBConstants.INDICATOR_4HOUR_TBL, DBConstants.INDICATOR_COLUMNS)
        return chartXminHelper, indicatorXminHelper

    elif Xmin == 1440:
        chartXminHelper = DBHelper(dbPass, DBConstants.CHART_1DAY_TBL, DBConstants.CHART_COLUMNS)
        return chartXminHelper

    else:
        return None

# 指定された時間間隔用のDBHelperを作成
def createIndicatorXminHelper(dbPass, Xmin):

    if Xmin == 1:
        indicatorXminHelper = DBHelper(dbPass, DBConstants.INDICATOR_1MIN_TBL, DBConstants.INDICATOR_COLUMNS)
        return indicatorXminHelper

    elif Xmin == 5:
        indicatorXminHelper = DBHelper(dbPass, DBConstants.INDICATOR_5MIN_TBL, DBConstants.INDICATOR_COLUMNS)
        return indicatorXminHelper

    elif Xmin == 10:
        indicatorXminHelper = DBHelper(dbPass, DBConstants.INDICATOR_10MIN_TBL, DBConstants.INDICATOR_COLUMNS)
        return indicatorXminHelper

    elif Xmin == 30:
        indicatorXminHelper = DBHelper(dbPass, DBConstants.INDICATOR_30MIN_TBL, DBConstants.INDICATOR_COLUMNS)
        return indicatorXminHelper

    elif Xmin == 60:
        indicatorXminHelper = DBHelper(dbPass, DBConstants.INDICATOR_1HOUR_TBL, DBConstants.INDICATOR_COLUMNS)
        return indicatorXminHelper

    elif Xmin == 240:
        indicatorXminHelper = DBHelper(dbPass, DBConstants.INDICATOR_4HOUR_TBL, DBConstants.INDICATOR_COLUMNS)
        return indicatorXminHelper

    elif Xmin == 1440:
        indicatorXminHelper = DBHelper(dbPass, DBConstants.INDICATOR_1DAY_TBL, DBConstants.INDICATOR_COLUMNS)
        return indicatorXminHelper

    else:
        return None


if __name__ == "__main__":

    inTable = ""
    outTable = ""
    add2DB()