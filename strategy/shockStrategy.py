# encoding: utf-8
from strategy.baseStrategy import baseStrategy
from manage.bitmexManage import bitmexManage
from sandbox.bitmexSandbox import bitmexSandbox

from decimal import Decimal


class shockStrategy(baseStrategy, bitmexManage, bitmexSandbox):
    MIN_NUM = 50
    MIN_PRICE = 0.5

    # 上下震荡策略
    def __init__(self, user, pub_redis, pri_redis, symbol):
        bitmexSandbox.__init__(self)
        bitmexManage.__init__(self, user, pub_redis, pri_redis, symbol)
        baseStrategy.__init__(self)  # 最优先

        self.allCost = self.data['margin']['allCost']
        self.lv = bitmexManage.LV
        self.posPrice = self.data['position']['price']
        self.posNum = self.data['position']['num']
        self.lastBuy = self.data['market']['latsBuy']
        self.lastSell = self.data['market']['latsSell']
        self.trust = self.data['order']

        self.start_position_buy = self.lastBuy + self.MIN_PRICE
        self.start_position_sell = self.lastSell - self.MIN_PRICE

        # 下单次数
        self.pairs = 3
        # 最大订单数 减去千分之5的数量保证 分开为2买卖双单保证金充足
        self.maxSize = int(self.allCost/100000000 * self.lastBuy * self.lv*(1-0.05)/2)
        # 最小订单数
        self.minSize = -self.maxSize

        # 以当前价维持价差
        self.maintain_spreads = False
        # 维持美元单位
        self.dollar_base = False
        # 维持价差百分比
        self.interval = 0.005
        # 第一仓位与当前价格的百分比 maintain_spreads 为true时生效
        self.min_spread = 0.0005
        # 价格相似
        self.price_like = round(10 / self.lastBuy, 3) if self.lastBuy > 0 else 0
        # 订单叠加数
        self.step_size = int((self.maxSize / self.pairs) * ((self.pairs - 1) / 2 / self.pairs))
        # 订单最小数
        self.start_size = int(self.maxSize / self.pairs - self.step_size * (self.pairs - 1) / 2)

        self.tickLog = Decimal(str(self.MIN_PRICE)).as_tuple().exponent * -1

        self.sign = 0

        self.new_order = {}

    # 设置沙盒
    @staticmethod
    def setRedis(conf):
        return bitmexSandbox.setBox(conf)

    # 创建订单
    def getOrder(self):
        self.get_ticker()
        if self.get_price_offset(-1) >= self.lastSell or self.get_price_offset(1) <= self.lastBuy:
            # raise Exception("Sanity check failed, exchange data is inconsistent")
            return
        new_order = self.place_orders()
        self.order.update(new_order)
        self.formatOrder()

        return self.order

    def get_ticker(self):
        if self.maintain_spreads:
            max_buy = 0
            min_sell = 0
            for i in self.trust:
                if i['side'] == 'Buy':
                    max_buy = i['price'] if i['price'] > max_buy else max_buy
                else:
                    min_sell = i['price'] if min_sell != 0 or i['price'] < min_sell else min_sell

            if 0 < max_buy == self.lastBuy:
                self.start_position_buy = self.lastBuy
            if 0 < min_sell == self.lastSell:
                self.start_position_sell = self.lastSell

        if self.start_position_buy * (1.00 + self.min_spread) > self.start_position_sell:
            self.start_position_buy *= (1.00 - (self.min_spread / 2))
            self.start_position_sell *= (1.00 + (self.min_spread / 2))

    def short_position_limit_exceeded(self):
        return self.posNum <= self.minSize

    def long_position_limit_exceeded(self):
        return self.posNum >= self.maxSize

    def place_orders(self):
        buy_orders = []
        sell_orders = []

        if self.posNum > 0:
            buy_num = self.posNum
            sell_num = 0
        else:
            buy_num = 0
            sell_num = self.posNum
        # reversed
        for i in range(1, self.pairs + 1):
            if not self.long_position_limit_exceeded():
                buy_order = self.prepare_order(-i)
                if buy_num >= buy_order['orderQty']:
                    buy_num -= buy_order['orderQty']
                else:
                    buy_order['orderQty'] = buy_order['orderQty'] - buy_num
                    buy_num = 0
                    if buy_order['orderQty'] < self.MIN_NUM:
                        continue
                    buy_orders.append(buy_order)
            if not self.short_position_limit_exceeded():
                sell_order = self.prepare_order(i)
                if sell_num <= -sell_order['orderQty']:
                    sell_num += sell_order['orderQty']
                else:
                    sell_order['orderQty'] = sell_order['orderQty'] + sell_num
                    sell_num = 0
                    if sell_order['orderQty'] < self.MIN_NUM:
                        continue
                    sell_orders.append(sell_order)

        if self.dollar_base:
            buy_orders = self.close_sell(buy_orders)
        return self.converge_orders(buy_orders, sell_orders)

    def close_sell(self, buy_orders):
        if not self.dollar_base:
            return buy_orders

        if self.posNum >= 0:
            return []

        new_buy = []
        buy_num = self.posNum.copy()
        for order in buy_orders:
            if order['orderQty'] + buy_num <= 0:
                new_buy.append(order)
                buy_num = order['orderQty'] + buy_num
            else:
                order['orderQty'] = -buy_num
                new_buy.append(order)
                break
        return new_buy

    def converge_orders(self, buy_orders, sell_orders):
        new_order = {}
        up_order = []
        add_order = []
        del_order = []
        buys_matched = 0
        sells_matched = 0

        for order in self.trust:
            try:
                if order['side'] == 'Buy':
                    desired_order = buy_orders[buys_matched]
                    buys_matched += 1
                else:
                    desired_order = sell_orders[sells_matched]
                    sells_matched += 1
                # TODO 当买仓被卖小于50时 买单不添加此单后卖单少一单
                if desired_order['orderQty'] != order['unDealNum'] or (desired_order['price'] != order['price'] and abs(
                        (desired_order['price'] / order['price']) - 1) > self.price_like):
                    up_order.append(
                        {'orderID': order['orderID'], 'orderQty': order['dealNum'] + desired_order['orderQty'],
                         'price': desired_order['price'], 'side': order['side']})
            except IndexError:
                del_order.append(order)

        while buys_matched < len(buy_orders):
            add_order.append(buy_orders[buys_matched])
            buys_matched += 1

        while sells_matched < len(sell_orders):
            add_order.append(sell_orders[sells_matched])
            sells_matched += 1

        if len(up_order) > 0:
            new_order['update'] = up_order
        if len(add_order) > 0:
            new_order['create'] = add_order
        if len(del_order) > 0:
            new_order['delete'] = del_order
        return new_order

    def prepare_order(self, index):
        quantity = self.start_size + ((abs(index) - 1) * self.step_size)

        price = self.get_price_offset(index)

        return {'symbol': self.symbol, 'price': price, 'orderQty': quantity, 'side': "Buy" if index < 0 else "Sell"}

    def get_price_offset(self, index):
        """
        给定一个索引（1，-1，2，-2，等等）返回其符号及索引所在位置的价格。负数表示买入，正值表示卖出
        """
        # 以仓位价维持价差
        if self.maintain_spreads:
            start_position = self.start_position_buy if index < 0 else self.start_position_sell
            # 第一个位置1，-1
            index = index + 1 if index < 0 else index - 1
        else:
            # 以盘口价维持价差
            start_position = self.start_position_buy if index < 0 else self.start_position_sell

            # 卖出价低于买入价，转卖
            if index > 0 and start_position < self.start_position_buy:
                start_position = self.start_position_sell
            # 买
            if index < 0 and start_position > self.start_position_sell:
                start_position = self.start_position_buy

        return self.toNearest(start_position * (1 + self.interval) ** index)

    def toNearest(self, num):
        tickDec = Decimal(str(self.MIN_PRICE))
        return float((Decimal(round(num / self.MIN_PRICE, 0)) * tickDec))
