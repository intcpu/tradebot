# encoding: utf-8
import datetime
import json
import time
import pandas as pd
import numpy as np

from chart.myPlot import myPlot
from client.redisClient import redisClient
from config.public import EVN_LIST
from manage.bitmexManage import bitmexManage
from margin.bitmexMargin import bitmexMargin
from market.bitmexMarket import bitmexMarket
from master import master
from order.bitmexOrder import bitmexOrder
from position.bitmexPosition import bitmexPosition
from sandbox.bitmexSandbox import bitmexSandbox


def dateToStr(dateline):
    timeArray = time.localtime(int(dateline))
    dateStr = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    return dateStr


class bitmexTestProcess:
    PRO_CNAME = 'bitmexTest'

    KLINE_FILE = {
        '1h': './data/xbt_bitmex_60.csv',
        '1d': './data/xbt_bitmex_1440.csv',
    }

    # 基础测试线
    BASE_DATE = '1h'

    # kline合并设置
    OHLC_DICT = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
    }

    # 初始金额 ubtc
    INIT_COST = 50000000

    def __init__(self):
        self.redis = {}
        self.margin = {'allCost': self.INIT_COST, 'useCost': self.INIT_COST}
        self.position = {'symbol': '', 'lv': bitmexManage.LV}
        self.strategy = None
        self.kline = {}
        self.dataframe = {}

    def init(self):
        master.set_pro_name(self.PRO_CNAME)
        self.initBitmex()

        from config.user import USER_CONFIG
        for conf in USER_CONFIG:
            user = conf['user']
            name = user['name']
            symbol = conf['symbol']

            pri_redis = self.redis[name + str(conf['sandbox'])]
            pub_redis = self.redis[conf['site'] + str(user['test_net'])]

            baseBoxDate = bitmexSandbox.KLINE_LIST[0]

            # 从沙盒120根基础k线运行时间开始 当前测试k线
            s_date = self.dataframe[baseBoxDate].iloc[0:bitmexMarket.MAX_LINE].index[-1]
            # 2019-07-03 11:00:00  5m线 第一个成交
            for index, row in self.dataframe[self.BASE_DATE].loc[s_date:].iterrows():
                self.updateNowKline(index, row)

                if len(self.kline[baseBoxDate]) < bitmexMarket.MAX_LINE:
                    continue
                if len(self.kline[baseBoxDate]) > bitmexMarket.MAX_LINE:
                    self.kline[baseBoxDate] = self.kline[baseBoxDate][-bitmexMarket.MAX_LINE:]

                # 获取策略
                self.strategy = conf['strategy'](user, pub_redis, pri_redis, symbol)
                self.strategy.realMode = False
                self.strategy.backMode = True

                if self.strategy.realMode:
                    sell = row['open']
                else:
                    sell = 0
                self.setBitmexMarket(pub_redis, symbol, row['close'], sell)
                self.setBitmexKline(pub_redis, symbol)

                order = self.strategy.getOrder()
                self.strategy.runBox()

                # todo 图表观察
                # myPlot.kline(self.kline['1h'])

        master.stop()
        master.kill_me()

    # redis
    def initBitmex(self):
        from config.user import USER_CONFIG

        for conf in USER_CONFIG:
            user = conf['user']
            name = user['name']

            self.position['symbol'] = conf['symbol']

            pub_key = conf['site'] + str(user['test_net'])
            if pub_key not in self.redis:
                redis_pub = EVN_LIST['test' if user['test_net'] else 'line'][conf['site']]
                self.redis[pub_key] = redisClient(namespace=redis_pub)
            pri_key = name + str(conf['sandbox'])
            if name not in self.redis:
                self.redis[pri_key] = redisClient(namespace=conf['user']['redis_pri'])

            # 删除策略订单
            self.redis[pub_key].hdelete('order:parbin')
            self.initXbtData()
            self.initBitmexMargin(pri_key, conf['symbol'])

    # 回测xbt的数据
    def initXbtData(self):
        baseDate = self.BASE_DATE
        if self.KLINE_FILE[baseDate].find('bitmex') > 0:
            # 时间戳
            self.dataframe[baseDate] = pd.read_csv(self.KLINE_FILE[baseDate], header=None, index_col=0, parse_dates=['timestamp'],
                                                   date_parser=dateToStr,
                                                   usecols=[0, 1, 2, 3, 4, 5], names=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        else:
            self.dataframe[baseDate] = pd.read_csv(self.KLINE_FILE[baseDate], header=None, index_col=0, parse_dates=['timestamp'],
                                                   usecols=[0, 1, 2, 3, 4, 5], names=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        for date in bitmexSandbox.KLINE_LIST:
            if date == baseDate:
                continue
            # 重组数据并删除含有nan的数据
            self.dataframe[date] = self.dataframe[baseDate].resample(date, closed='left', label='left').agg(self.OHLC_DICT).dropna(axis=0, how='any')

    # 初始化 仓位 订单 保证金
    def initBitmexMargin(self, name, symbol):
        margin = {}
        position = {}
        for k, v in bitmexMargin.key_map.items():
            margin[k] = self.margin[v] if v in self.margin else 0
        for k, v in bitmexPosition.key_map.items():
            position[k] = self.position[v] if v in self.position else 0

        self.redis[name].set(bitmexMargin.KEY, json.dumps(margin))
        self.redis[name].hset(bitmexOrder.KEY, symbol, json.dumps([]))
        self.redis[name].hset(bitmexPosition.KEY, symbol, json.dumps([position]))

    # 同步历史k线至当前运行时间
    def updateNowKline(self, index, row):
        baseDate = self.BASE_DATE
        nowTime = index
        for k, v in enumerate(bitmexSandbox.KLINE_LIST):
            # 小于当前时间的120根k线
            kline = self.dataframe[v].loc[:nowTime]
            kline = kline[-bitmexMarket.MAX_LINE:] if len(kline) > bitmexMarket.MAX_LINE else kline
            if len(kline) == 0:
                continue
            # 同步最后一根k线为实时数据
            toNow = self.dataframe[baseDate].loc[kline.index[-1]:nowTime]
            idx = kline.index[-1]
            kline.at[idx, 'high'] = toNow['high'].max()
            kline.at[idx, 'low'] = toNow['low'].min()
            kline.at[idx, 'close'] = toNow['close'][-1]
            kline.at[idx, 'volume'] = toNow['volume'].sum()
            self.kline[v] = kline

    # 设置市价
    def setBitmexMarket(self, pub_redis, symbol, buy, sell=0):
        market = {}
        for k, v in bitmexMarket.instrument_map.items():
            if v in ['nowFree', 'nextFree']:
                market[k] = 0
            elif v == 'latsBuy':
                market[k] = int(buy) if sell == 0 else buy
            elif v == 'latsSell':
                market[k] = (int(buy) + 1) if sell == 0 else sell
            else:
                market[k] = buy
        pub_redis.hset(bitmexMarket.INSTRUMENT, symbol, json.dumps([market]))

    # 设置k线
    def setBitmexKline(self, pub_redis, symbol):
        for i in bitmexSandbox.KLINE_LIST:
            klineKey = bitmexMarket.KLINE + i
            kline = self.kline[i].reset_index()
            kline['timestamp'] = [x.strftime('%Y-%m-%d %H:%M:%S') for x in kline['timestamp']]
            pub_redis.hset(klineKey, symbol, json.dumps(kline.to_dict(orient='records')))
            pub_redis.hdelete(bitmexMarket.TRADEBIN + i)
