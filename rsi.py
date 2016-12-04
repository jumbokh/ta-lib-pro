#coding:utf-8

import csv
import time
import datetime
import talib
import pandas
import numpy as np

#1分足のチャートからX分足のチャートを作成する
#TODO 生成できなかった時刻がある場合はそれを警告
def checkGoldenCross(inFileName, outFileName):

    prices = np.loadtxt(inFileName, delimiter=",", usecols=(1, 2, 3, 4))

    print(prices)
    wf = open(outFileName, 'a') #ファイルが無ければ作る、の'a'を指定
    dataWriter = csv.writer(wf,lineterminator='\n')

    sma5 = talib.SMA(prices[:, 3], timeperiod=5)    #SMA
    sma25 = talib.SMA(prices[:, 3], timeperiod=25)
    rsi9 = talib.RSI(prices[:, 3], timeperiod=9)    #RSI(close, timeperiod=9)
    rsi11 = talib.RSI(prices[:, 3], timeperiod=11)
    rsi14 = talib.RSI(prices[:, 3], timeperiod=14)
    macd, macdsignal, macdhist = talib.MACD(prices[:, 3], fastperiod=12, slowperiod=26, signalperiod=9) #MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
    cci14 = talib.CCI(prices[:, 1], prices[:, 2], prices[:, 3], timeperiod=14)  #CCI(high, low, close, timeperiod=14)
    cci20 = talib.CCI(prices[:, 1], prices[:, 2], prices[:, 3], timeperiod=20)
    slowk, slowd = talib.STOCH(prices[:, 2], prices[:, 1], prices[:, 3], fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3,slowd_matype=0)

    total = 100000
    units = 1000

    for i in range(len(prices) - 1):

        dataWriter.writerow([prices[i,0], prices[i,1], prices[i,2], prices[i,3], sma5[i], sma25[i], rsi9[i], rsi11[i], rsi14[i], macd[i], macdsignal[i], macdhist[i], cci14[i], cci20[i]])

        if ((sma5[i] < sma25[i]) and (sma5[i + 1] > sma25[i + 1])):
            print("Golden Closs!!")
            total = total - 1000 * prices[i, 3]

        elif ((sma5[i] > sma25[i]) and (sma5[i + 1] < sma25[i + 1])):
            print("Dead Closs!!")
            total = total + 1000 * prices[i, 3]


    print(total)
    print(talib.get_function_groups())

if __name__ == "__main__":
    checkGoldenCross("/home/k-sui/fxdata/1h_2000-2015.csv","/home/k-sui/fxdata/1h_2000-2015_indicator.csv")