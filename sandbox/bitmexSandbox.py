# encoding: utf-8
import json
import logging
import time

from client.redisClient import redisClient
from manage.bitmexManage import bitmexManage

from margin.bitmexMargin import bitmexMargin
from market.bitmexMarket import bitmexMarket
from master import master
from order.bitmexOrder import bitmexOrder
from sandbox.baseSandbox import baseSandbox


class bitmexSandbox(baseSandbox):
    # 单位换算
    FLOAT_SIZE = 100000000
    # 重置保证金
    resetMargin = True
    # 沙盒模拟真实环境能获取的k线
    KLINE_LIST = ['1h', '1d']

    def __init__(self):
        super().__init__()
        self.runId = int(time.time_ns()/1000)

        self.symbol = None
        self.order = None
        self.kline = None
        self.lastKline = None
        self.data = None
        self.pri_redis = None
        # 成交模式: 真实 or k线
        self.realMode = True
        # 回测模式 or 实时模式
        self.backMode = False

    # 设置用户沙盒环境
    @staticmethod
    def setBox(conf=None):
        key = conf['user']['redis_pri']
        sandbox = conf['sandbox']
        redis = redisClient(namespace=key)
        box = redisClient(namespace=key, select_db=sandbox)

        if not bitmexSandbox.resetMargin and box.get('time'):
            return box

        for key, obj in bitmexManage.MAPS.items():
            if key == bitmexMarket.KEY:
                continue
            elif key == bitmexMargin.KEY:
                data = redis.get(obj.KEY)
                box.set(obj.KEY, data)
                data = bitmexManage.MAPS[key].get(box)
                for k, v in data.items():
                    if k == 'useCost':
                        data[k] = data['allCost']
                    if k not in ['useCost', 'allCost']:
                        data[k] = 0
                data = bitmexSandbox.mapData(key, data)
                box.set(obj.KEY, json.dumps(data))
            else:
                data = redis.hgetall(obj.KEY)
                if not data:
                    continue
                box.hmset(obj.KEY, data)
                data = bitmexManage.MAPS[key].get(box, conf['symbol'])
                if key == bitmexOrder.KEY:
                    data = []
                else:
                    for k, v in data.items():
                        if k not in ['symbol', 'lv', 'allCost']:
                            data[k] = 0
                    data = [bitmexSandbox.mapData(key, data)]
                box.hset(obj.KEY, conf['symbol'], json.dumps(data))
        return box

    # 反转数据
    @staticmethod
    def mapData(key, data):
        if not data:
            return data
        map_data = {}
        for mk, mv in bitmexManage.MAPS[key].key_map.items():
            if mv in data:
                map_data[mk] = data[mv]
        if not map_data:
            logging.info({'title': '沙盒数据异常', 'key': key, 'data': data, 'maps': bitmexManage.MAPS[key].key_map})

        return map_data

    # 运行沙盒
    def runBox(self):
        if not self.kline or len(self.kline[self.KLINE_LIST[0]]) < 1:
            return
        self.lastKline = self.kline[self.KLINE_LIST[0]][-1]

        # todo 沙盒晚处理0信号单 导致重复平仓  平仓后再成交平仓单
        # 用k线模拟成交需隔一段时间 回测时不需要
        if self.isMarketByKline():
            return

        if not self.boxCheckMargin():
            return

        if len(self.order) > 2:
            logging.info({'id': self.runId, 'title': '沙盒准备处理', 'orders': self.order, 'data': self.data, 'kline': self.lastKline})

        self.boxCheckOrder()
        self.boxCheckPosition()
        self.boxCreateOrder()
        self.boxOrderMarket()
        self.boxSyncData()

        if len(self.order) > 2:
            logging.info({'id': self.runId, 'title': '沙盒处理完成', 'orders': self.order, 'data': self.data, 'kline': self.lastKline})

        self.pri_redis.set('time', self.getlastTime())

    # 检查沙盒资金
    def boxCheckMargin(self):
        errMsg = ''
        for k, v in self.data['margin'].items():
            if v < 0:
                errMsg = '沙盒资产计算错误'
        # if self.data['margin']['orderCost'] > 0 and len(self.data['order']) == 0:
        #     errMsg = '沙盒订单资产错误'
        maxNum = int(self.data['margin']['allCost']*self.data['position']['lv']*self.data['market']['lastPrice']/self.FLOAT_SIZE)
        if maxNum < bitmexManage.MIN_NUM:
            errMsg = '沙盒全仓已爆仓'

        if errMsg:
            logging.info({'id': self.runId, 'errMsg': errMsg, 'orders': self.order, 'data': self.data, 'kline': self.lastKline})
            master.stop()
            master.kill_me()

        return True

    # 根据k线价格成交 是否更新到最新价格
    def isMarketByKline(self):
        if self.realMode or self.backMode:
            return False

        timeStamp = self.getlastTime()

        lastTime = self.pri_redis.get('time')
        lastTime = int(lastTime) if lastTime else 0

        return timeStamp - lastTime < 60

    # 获取最后k线时间戳
    def getlastTime(self):
        lastTime = self.lastKline['timestamp']
        if self.backMode:
            return lastTime

        formatStr = "%Y-%m-%d %H:%M:%S" if self.backMode else "%Y-%m-%dT%H:%M:%S.%fZ"
        timeArray = time.strptime(lastTime, formatStr)
        timeStamp = int(time.mktime(timeArray)) + time.localtime().tm_gmtoff

        return int(self.runId/1000000) if self.realMode else timeStamp

    # 同步最新沙盒数据
    def boxSyncData(self):
        for key, obj in self.MAPS.items():
            if key == bitmexMarket.KEY:
                continue
            if key == bitmexMargin.KEY:
                data = bitmexSandbox.mapData(key, self.data[key])
                self.pri_redis.set(obj.KEY, json.dumps(data))
            else:
                if key == bitmexOrder.KEY:
                    data = [bitmexSandbox.mapData(key, i) for i in self.data[key]]
                else:
                    data = [bitmexSandbox.mapData(key, self.data[key])]
                self.pri_redis.hset(obj.KEY, self.symbol, json.dumps(data))

    # 行情变动仓位检查
    def boxCheckPosition(self):
        if self.realMode:
            lastKline = {'low': self.data['market']['latsBuy'], 'high': self.data['market']['latsSell']}
        elif self.kline[self.KLINE_LIST[0]]:
            lastKline = self.lastKline
        else:
            return

        if self.data['position']['num'] > 0 and self.data['position']['liquiPrice'] >= lastKline['low']:
            self.boxBurst()
        if self.data['position']['num'] < 0 and self.data['position']['liquiPrice'] <= lastKline['high']:
            self.boxBurst()

    # 行情变动订单
    def boxCheckOrder(self):
        if self.realMode:
            lastKline = {'low': self.data['market']['latsBuy'], 'high': self.data['market']['latsSell']}
        elif self.kline[self.KLINE_LIST[0]]:
            lastKline = self.lastKline
        else:
            return
        for i, o in enumerate(self.data['order']):
            orderQty = o['allNum']
            price = 0
            if 'price' in o and o['price'] > 0:
                if o['side'] == 'Buy' and o['price'] >= lastKline['low']:
                    price = o['price']
                if o['side'] == 'Sell' and o['price'] <= lastKline['high']:
                    price = o['price']
            elif 'stopPx' in o and o['stopPx'] > 0:
                if o['side'] == 'Buy' and o['stopPx'] <= lastKline['high']:
                    price = o['stopPx']
                elif o['side'] == 'Sell' and o['stopPx'] >= lastKline['low']:
                    price = o['stopPx']
            if price > 0:
                num = orderQty if o['side'] == 'Buy' else -orderQty
                # 减仓订单不能成交
                if o['isReduce'] == 1 and (num * self.data['position']['num']) >= 0:
                    continue

                del self.data['order'][i]
                self.boxOrderDeal(price, num)

                logging.info({'id': self.runId, 'title': '沙盒限价成交', 'orders': self.order, 'data': self.data})

    # 爆仓
    def boxBurst(self):
        self.data['margin']['allCost'] -= self.data['margin']['posCost']
        self.data['margin']['posCost'] = 0
        self.data['position']['price'] = 0
        self.data['position']['num'] = 0
        self.data['position']['margin'] = 0
        self.data['position']['liquiPrice'] = 0
        self.data['position']['cost'] = 0
        self.data['position']['unGet'] = 0
        self.data['position']['unGetRate'] = 0
        logging.info({'id': self.runId, 'title': '沙盒爆仓完成', 'orders': self.order, 'data': self.data})

    # 沙盒下单
    def boxCreateOrder(self):
        self.delBoxOrder()
        self.updateBoxOrder()
        self.addBoxOrder()

    # 删除订单
    def delBoxOrder(self):
        if 'delete' not in self.order:
            return
        self.order['delete'] = json.loads(self.order['delete'])
        for d in self.order['delete']:
            for i, o in enumerate(self.data['order']):
                if d['orderID'] == o['orderID']:
                    del self.data['order'][i]
                    if 'price' in o:
                        margin = bitmexSandbox.getOrderMargin(o['unDealNum'], o['price'], self.data['position']['lv'])
                        self.data['margin']['orderCost'] -= margin
                        self.data['margin']['useCost'] += margin
        logging.info({'id': self.runId, 'title': '沙盒删除订单', 'orders': self.order, 'data': self.data})

    # 更新订单
    def updateBoxOrder(self):
        if 'update' not in self.order:
            return
        self.order['update'] = json.loads(self.order['update']) if type(self.order['update']) == str else self.order['update']
        for u in self.order['update']:
            for i, o in enumerate(self.data['order']):
                if u['orderID'] == o['orderID']:
                    # 更新订单价格及数量
                    if 'orderQty' in u:
                        upOrder = {'price': u['price'], 'allNum': u['orderQty'], 'unDealNum': (u['orderQty'] - o['dealNum'])}
                    else:
                        upOrder = {'price': u['price'], 'allNum': o['allNum'], 'unDealNum': (o['allNum'] - o['dealNum'])}
                    # 计算新订单差额保证金
                    newMargin = bitmexSandbox.getOrderMargin(upOrder['unDealNum'], u['price'], self.data['position']['lv'])
                    oldMargin = bitmexSandbox.getOrderMargin(o['allNum'], o['price'], self.data['position']['lv'])
                    margin = int(newMargin - oldMargin)

                    if self.data['margin']['useCost'] < margin:
                        continue

                    o.update(upOrder)
                    self.data['order'][i] = o
                    self.data['margin']['orderCost'] += margin
                    self.data['margin']['useCost'] -= margin
        logging.info({'id': self.runId, 'title': '沙盒更新订单', 'orders': self.order, 'data': self.data})

    # 新增订单
    def addBoxOrder(self):
        if 'create' not in self.order:
            return
        self.order['create'] = json.loads(self.order['create'])
        for c in self.order['create']:
            if 'price' in c:
                margin = bitmexSandbox.getOrderMargin(c['orderQty'], c['price'], self.data['position']['lv'])
                if self.data['margin']['useCost'] < margin:
                    continue
                self.data['margin']['orderCost'] += margin
                self.data['margin']['useCost'] -= margin

            # 反转字段创建订单
            order = {'orderID': int(time.time_ns() / 1000), 'symbol': c['symbol'], 'side': c['side'], 'allNum': c['orderQty'], 'unDealNum': c['orderQty'], 'dealNum': 0}
            if 'price' in c:
                order['price'] = c['price']
            elif 'stopPx' in c:
                order['stopPx'] = c['stopPx']
            if 'execInst' in c:
                order['isReduce'] = c['execInst']
            self.data['order'].append(order)
        logging.info({'id': self.runId, 'title': '沙盒新增订单', 'orders': self.order, 'data': self.data})

    # 市价单成交
    def boxOrderMarket(self):
        if not self.realMode:
            return
        for i, o in enumerate(self.data['order']):
            orderQty = o['allNum']
            price = 0
            if 'price' in o and o['price'] > 0:
                if o['side'] == 'Buy' and o['price'] >= self.data['market']['latsSell']:
                    price = self.data['market']['latsSell']
                if o['side'] == 'Sell' and o['price'] <= self.data['market']['latsBuy']:
                    price = self.data['market']['latsBuy']
            elif 'stopPx' in o and o['stopPx'] > 0:
                if o['side'] == 'Buy' and o['stopPx'] <= self.data['market']['latsBuy']:
                    price = self.data['market']['latsSell']
                elif o['side'] == 'Sell' and o['stopPx'] >= self.data['market']['latsSell']:
                    price = self.data['market']['latsBuy']
            else:
                price = self.data['market']['latsSell'] if o['side'] == 'Buy' else self.data['market']['latsBuy']

            if price > 0:
                del self.data['order'][i]
                num = orderQty if o['side'] == 'Buy' else -orderQty
                self.boxOrderDeal(price, num)
                logging.info({'id': self.runId, 'title': '沙盒市价成交', 'orders': self.order, 'data': self.data})

    # 成交处理
    def boxOrderDeal(self, price, orderQty):
        if price == 0 or orderQty == 0:
            return
        position = self.data['position'].copy()
        buySell = position['num'] * orderQty
        self.data['position']['num'] += orderQty
        # 加仓
        if buySell >= 0:
            positionPrice = (position['price'] * position['num'] + price * orderQty) / (position['num'] + orderQty)
            self.data['position']['price'] = round(positionPrice, 2)
        # 反向开仓
        elif buySell < 0:
            # 仓位反转
            if self.data['position']['num']*orderQty > 0:
                self.data['position']['price'] = price
                profit = (position['num']/position['price'] - position['num']/price) * self.FLOAT_SIZE
            # 减仓
            elif self.data['position']['num']*orderQty < 0:
                profit = (orderQty/price - orderQty/position['price']) * self.FLOAT_SIZE
            # 平仓
            else:
                self.data['position']['price'] = 0
                profit = (position['num'] / position['price'] - position['num'] / price) * self.FLOAT_SIZE

            self.data['margin']['allCost'] += int(profit)

        self.resetUserMargin()

    # 重置用户保证金
    def resetUserMargin(self):
        self.data['position']['margin'] = bitmexSandbox.getOrderMargin(self.data['position']['num'], self.data['position']['price'], self.data['position']['lv'])

        if self.data['position']['num'] != 0:
            self.data['position']['liquiPrice'] = bitmexSandbox.getLiquiPrice(self.data['position']['price'], self.data['position']['lv'], self.data['position']['num'])
        else:
            self.data['position']['liquiPrice'] = 0

        self.data['margin']['orderCost'] = 0
        for i in self.data['order']:
            self.data['margin']['orderCost'] += bitmexSandbox.getOrderMargin(i['unDealNum'], i['price'], self.data['position']['lv'])

        self.data['margin']['posCost'] = self.data['position']['margin']
        self.data['margin']['useCost'] = self.data['margin']['allCost'] - self.data['margin']['posCost'] - self.data['margin']['orderCost']

    # 订单保证金
    @staticmethod
    def getOrderMargin(orderQty, price, lv):
        if orderQty == 0 or price == 0:
            return 0
        lv = 100 if lv == 0 else lv
        num = abs(orderQty)
        return int(((num / price)*bitmexSandbox.FLOAT_SIZE)/lv)

    # 爆仓价 20200724 核算精确
    @staticmethod
    def getLiquiPrice(price, lv, num):
        # 维持保证金
        keep = 0.004
        # 平仓手续费
        fee = 0.00025
        # 多空方向
        direction = num / abs(num)
        return price*direction/(1/lv+direction-keep-fee)
