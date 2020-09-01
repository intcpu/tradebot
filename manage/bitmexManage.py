# encoding: utf-8
import json
import logging
import time

from manage.baseManage import baseManage
from margin.bitmexMargin import bitmexMargin
from market.bitmexMarket import bitmexMarket
from order.bitmexOrder import bitmexOrder
from position.bitmexPosition import bitmexPosition


class bitmexManage(baseManage):
    # 数据获取
    MAPS = {
        'position': bitmexPosition,
        'order': bitmexOrder,
        'margin': bitmexMargin,
        'market': bitmexMarket,
    }

    # 买卖标示
    SIDE_MAPS = {
        'Buy': 1,
        'Sell': 2,
    }

    # 真实环境能获取获取的k线
    KLINE_LIST = ['1h', '1d']

    # 最大开仓保证金比
    MAX_LOSS = 0.3

    # 最大开单数
    MAX_STEP = 1
    # 订单更新价比
    DIFF_PRICE_RATE = 0.0005
    # 杠杆
    LV = 10
    # 最小开仓数量
    MIN_NUM = 50
    # 市价追单的盘口差价比
    MARKET_DIFF = 0.001

    def __init__(self, user, pub_redis, pri_redis, symbol):
        baseManage.__init__(self)

        self.nowTime = int(time.time_ns()/1000)

        self.user = user
        self.symbol = symbol
        self.pub_redis = pub_redis
        self.pri_redis = pri_redis

        # 基础数据
        self.data = {
            'position': bitmexManage.MAPS['position'].get(self.pri_redis, self.symbol),
            'order': bitmexManage.MAPS['order'].get(self.pri_redis, self.symbol),
            'margin': bitmexManage.MAPS['margin'].get(self.pri_redis),
            'market': bitmexManage.MAPS['market'].get(self.pub_redis, self.symbol)
        }

        # 是否市价下单
        self.isMarket = abs(self.data['market']['latsSell'] - self.data['market']['latsBuy']) >= int(self.data['market']['lastPrice'] * self.MARKET_DIFF)

        # 最大开仓数 todo self.LV 需与仓位杠杆相同 无仓位时 self.data['position']['lv']为0
        self.maxSize = int((self.data['margin']['allCost']/100000000) * self.data['market']['latsBuy'] * self.LV*(1-0.05))

        # 当前开仓数
        self.size = int(self.maxSize*self.MAX_LOSS)

        # 开单信息初始化
        self.kline = {}
        self.strategySign = -1
        self.price = 0

        # 设置开单
        self.order = {'time': self.nowTime}

        # 当前订单信息
        self.orderSize = {'Buy': 0, 'Sell': 0}
        self.setBaseInfo()

        # 设置k线
        self.setKline()

    # 当前k线数据
    def setKline(self):
        for date in self.KLINE_LIST:
            if date not in self.kline:
                self.kline[date] = []
            if len(self.kline[date]) == 0:
                self.kline[date] = bitmexManage.MAPS['market'].kline(self.pub_redis, date, self.symbol)
        return self.kline

    # 当前仓位及订单信息
    def setBaseInfo(self):
        baseInfo = ['position_{}_{}'.format(self.data['position']['price'], self.data['position']['num'])]
        for i in self.data['order']:
            if not i:
                continue
            baseInfo.append('{}_{}_{}_{}'.format(i['orderID'], i['side'], i['price'], i['stopPx'], i['unDealNum']))
            self.orderSize[i['side']] += i['unDealNum']
        self.order['info'] = json.dumps([k for k in sorted(baseInfo)])

    # 获取订单
    def setOrder(self, sign):
        self.strategySign = sign
        if self.strategySign < 0:
            return {}
        self.setPrice()
        self.updateOrder()
        self.createOrder()
        self.formatOrder()

        return self.order

    def formatOrder(self):
        orders = {}
        for key, order in self.order.items():
            if type(order) in [int, str]:
                orders[key] = order
            else:
                if type(order) == dict:
                    order = {i: v if type(v) == str else float(v) for i, v in order.items()}
                orders[key] = json.dumps(order)
        self.order = orders

    # 设置价格
    def setPrice(self):
        if self.strategySign == 1:
            self.price = self.data['market']['latsSell']
        elif self.strategySign == 2:
            self.price = self.data['market']['latsBuy']
        else:
            self.price = self.getClosePrice()
        return self.price

    # 获取平仓价
    def getClosePrice(self):
        closePrice = int(self.data['position']['price'])
        if self.isMarket:
            closePrice = self.data['market']['latsBuy'] if self.data['position']['num'] > 0 else self.data['market']['latsSell']
        else:
            # 以更优的价格平仓
            if self.data['position']['num'] > 0:
                closePrice = self.data['market']['latsSell'] if self.data['market']['latsSell'] > closePrice else closePrice
            else:
                closePrice = self.data['market']['latsBuy'] if self.data['market']['latsBuy'] < closePrice else closePrice
        return closePrice

    # 开仓单
    def createOrder(self):
        createSize = self.getCreateSize()
        size = abs(createSize)
        if createSize == 0 or size < self.MIN_NUM:
            return

        order = {'symbol': self.symbol, 'price': self.price, 'orderQty': size, 'side': "Buy" if createSize > 0 else "Sell"}

        # 开仓与已存在订单删除冲突
        if 'delete' in self.order:
            delete = self.order['delete'].copy()
            for k, v in enumerate(delete):
                if v['price'] == order['price'] and v['unDealNum'] == order['orderQty'] and v['side'] == order['side']:
                    del delete[k]
            self.order['delete'] = delete

        if len(order) > 0:
            self.order['create'] = [order]

    # 获取开仓总数
    def getCreateSize(self):
        if self.strategySign == 1:
            baseSize = self.size - self.orderSize['Buy']
        elif self.strategySign == 2:
            baseSize = self.orderSize['Sell'] - self.size
        else:
            baseSize = 0
        createSize = baseSize - self.data['position']['num']

        return createSize

    # 更新单
    def updateOrder(self):
        for i in self.data['order']:
            if self.strategySign == self.SIDE_MAPS[i['side']]:
                # 差价太小不更新价格
                if abs(self.price - i['price']) > int(self.price*self.DIFF_PRICE_RATE):
                    if 'update' not in self.order:
                        self.order['update'] = []
                    self.order['update'].append({'orderID': i['orderID'], 'price': self.price})
            else:
                if 'delete' not in self.order:
                    self.order['delete'] = []
                self.order['delete'].append(i)


