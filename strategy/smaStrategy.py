# encoding: utf-8
import logging
import sys
import time
import traceback

import talib.abstract as ta
import pandas as pd
from strategy.baseStrategy import baseStrategy
from manage.bitmexManage import bitmexManage
from sandbox.bitmexSandbox import bitmexSandbox


class smaStrategy(baseStrategy, bitmexManage, bitmexSandbox):

    KLINE_LIST = ['5m']

    # 差价比
    DIFF_POINT = 0.0005

    # 指标设置 12m
    INDEX_MAP = {
                '1m': '12T',
                '5m': '15T',
                '1h': '4H',
                '1d': '1d',
                }
    # 指标基础时间
    INDEX_TIME = {
                '1m': 69,
                '5m': 309,
                '1h': 3609,
                '1d': 86409,
                }

    # 指标基础超时比
    INDEX_TIMEOUT = 1.2

    def __init__(self, user, pub_redis, pri_redis, symbol):
        # importlib.reload(strategy.baseStrategy)
        # importlib.reload(manage.bitmexManage)
        # importlib.reload(sandbox.bitmexSandbox)
        bitmexSandbox.__init__(self)
        bitmexManage.__init__(self, user, pub_redis, pri_redis, symbol)
        baseStrategy.__init__(self)  # 最优先

        # 使用指标
        self.kline_sign = self.KLINE_LIST[0]

        # 保持
        self.sign = -1
        self.ma5 = {}
        self.ma10 = {}

    # 设置沙盒
    @staticmethod
    def setRedis(conf):
        return bitmexSandbox.setBox(conf)

    # 4小时 5sma 10sma 策略
    def setSma(self):
        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            # 'avg': 'mean',
        }
        try:
            if not self.kline:
                return
            for date in self.kline:
                if date not in self.INDEX_MAP or not self.kline[date]:
                    continue
                key = self.INDEX_MAP[date]
                df = pd.DataFrame(self.kline[date], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

                if not self.checkTimeReal(df, date):
                    return

                # 均价
                # df['avg'] = (df['open']+df['close'])/2+df['high']-df['low']
                df.index = pd.DatetimeIndex(df.timestamp)

                # 压缩k线
                if not self.backMode and date != key:
                    df = df.resample(key, closed='left', label='left').agg(ohlc_dict)

                # 10均线
                if df.shape[0] > 10:
                    self.ma5[date] = ta.SMA(df, timeperiod=5, price='close')
                    self.ma10[date] = ta.SMA(df, timeperiod=10, price='close')

        except:
            t, v, tb = sys.exc_info()
            text = "".join(
                traceback.format_exception(t, v, tb)
            )
            logging.info(text)

    # 检查k线是否实时
    def checkTimeReal(self, df, date):
        if self.backMode:
            return True
        # 确认k线数据实时
        lastTime = df.timestamp.iloc[-1]
        if lastTime[-1] == 'Z':
            timeArray = time.strptime(lastTime, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            timeArray = time.strptime(lastTime, "%Y-%m-%d %H:%M:%S")
        timeStamp = int(time.mktime(timeArray)) + time.localtime().tm_gmtoff
        timeOut = int(self.INDEX_TIME[date] * self.INDEX_TIMEOUT)
        if int(time.time()) - timeStamp > timeOut:
            return False

        return True

    # 设置信号
    def setSign(self):
        if not self.ma5:
            return self.sign

        ma5 = self.ma5[self.kline_sign].iloc[-1]
        ma10 = self.ma10[self.kline_sign].iloc[-1]

        if pd.isna(ma5) or pd.isna(ma10):
            # todo 使用redis重启rest进程
            return 0

        diff = int(ma5*self.DIFF_POINT)
        # 开多
        if ma5 - ma10 >= diff:
            self.sign = 1
        # 开空
        elif ma10 - ma5 >= diff:
            self.sign = 2
        # 平仓
        else:
            self.sign = 0
        return self.sign

    # 获取订单
    def getOrder(self):

        self.setSma()
        self.setSign()
        self.setOrder(self.sign)

        return self.order

