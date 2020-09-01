# encoding: utf-8
import json
import logging
import time
import pandas as pd

from api.dingApi import dingApi
from strategy.baseStrategy import baseStrategy
from manage.bitmexManage import bitmexManage
from sandbox.bitmexSandbox import bitmexSandbox


class parbinStrategy(baseStrategy, bitmexManage, bitmexSandbox):
    # 策略使用k线
    KLINE_LIST = ['1h', '4H', '1d']

    # 盈亏比设置
    LOSS_RATE = 1.5

    # 关键点范围
    PRICE_RANGE = 100

    # 策略k线合成所用沙盒k线指标
    DATES = {
        '15T': '5m',
        '1h': '1h',
        '4H': '1h',
        '1d': '1d',
    }

    # 高低点统计范围
    HIGH_RANGE = {
        '15T': 0,
        '1h': 0,
        '4H': 6,
        '1d': 7,
    }
    # 小级别趋势统计范围
    TREND_RANGE = {
        '15T': 0,
        '1h': 3,
        '4H': 3,
        '1d': 0,
    }

    # 大级别趋势指标
    MAX_TREND_INDEX = '1d'
    # 大级别分析日期
    MAX_TREND_DAY = 60

    # 主要操作级别
    MAIN_TREND = '1h'
    # 合成观察级别
    WATCH_TREND = ''
    # 关键价格位级别
    PRICE_TREND = '4H'

    # kline合并设置
    OHLC_DICT = {
        'timestamp': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        # 'avg': 'mean',
    }

    def __init__(self, user, pub_redis, pri_redis, symbol):
        bitmexSandbox.__init__(self)
        bitmexManage.__init__(self, user, pub_redis, pri_redis, symbol)
        baseStrategy.__init__(self)  # 最优先

        # 钉钉
        self.dd = dingApi()

        # 操作信号
        self.sign = -1
        # 信号附加信息 级别、数量
        self.signInfo = {'sign': -1, 'date': '', 'num': 0, 'profit_price': 0, 'order_price': 0, 'stop_price': 0}
        # 信号线
        self.signLine = None

        # 策略操作线
        self.lines = {}
        # 小级别趋势点
        self.minTrendSpot = 0
        # 大级别趋势
        self.maxTrend = 0
        # 最高价
        self.maxPrice = 0
        # 最低价
        self.minPrice = 0
        # 关键价格
        self.keyPrice = {'up': [], 'down': []}

        # 主要信号
        self.mainSign = {
            # 关键位
            'keyLine': 0,
            # 趋势高低位
            'trendSpot': 0,
        }

        # 次要信号
        self.lessSign = {
            # 线明显
            'bigLine': 0,
            # 假突破
            'failBreak': 0,
            # 符合大趋势
            'inTrend': 0,
            # 左线吞没实体
            'noEntity': 0,
            # 盈亏比
            'profitRate': 0,
        }

    # 设置沙盒
    @staticmethod
    def setRedis(conf):
        return bitmexSandbox.setBox(conf)

    # 设置策略k线
    def setStrategyKline(self):
        for key in self.KLINE_LIST:
            date = self.DATES[key]
            if date not in self.kline:
                continue

            if date in self.lines:
                kline = self.lines[date].copy()
            else:
                kline = pd.DataFrame(self.kline[date],
                                     columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                kline.index = pd.DatetimeIndex(kline.timestamp)

            # 组合k线
            if date != key:
                kline = kline.resample(key, closed='left', label='left').agg(self.OHLC_DICT)

            if key == self.MAX_TREND_INDEX:
                kline = kline.iloc[-self.MAX_TREND_DAY:]

            self.lines[key] = kline

        if len(self.lines[self.MAIN_TREND]) < 3:
            raise ValueError('no MAIN_TREND kline')

    # 设置高低点指标
    def setBestWorst(self):
        for date, line in self.lines.items():
            # 指标范围
            count_num = self.HIGH_RANGE[date]
            if count_num == 0:
                continue
            self.lines[date]['best'] = 0
            self.lines[date]['worst'] = 0
            for i in range(count_num, len(line)):
                index = line.index[i]
                # 高点
                if line['high'][i] == line['high'][i - count_num:i + count_num].max():
                    self.lines[date].at[index, 'best'] = 1
                # 低点
                if line['low'][i] == line['low'][i - count_num:i + count_num].min():
                    self.lines[date].at[index, 'worst'] = 1
                # 平均震荡值
                # self.lines[date].at[index, 'shock'] = (line[i - count_num:i]['high']-line[i - count_num:i]['low']).mean()

    # 设置小级别趋势
    def setMinTrend(self):
        date = self.signInfo['date']
        num = self.signInfo['num']
        count_num = self.TREND_RANGE[date]
        line = self.lines[date]

        # 当前线不算
        line_len = len(line) - num
        # 小级别趋势
        start_len = line_len - count_num

        if self.sign == 1:
            # 买单->有小级别卖
            trend = sum([
                sum([1 if line.iloc[j]['open'] < line.iloc[j - 1]['open'] else 0 for j in range(start_len, line_len)]),
                sum([1 if line.iloc[j]['close'] < line.iloc[j - 1]['close'] else 0 for j in range(start_len, line_len)]),
            ])
            self.minTrendSpot = line.iloc[start_len:line_len]['low'].min() if trend >= (count_num*2-1) else 0
        elif self.sign == 2:
            # 卖单->有小级别买
            trend = sum([
                sum([1 if line.iloc[j]['open'] > line.iloc[j - 1]['open'] else 0 for j in range(start_len, line_len)]),
                sum([1 if line.iloc[j]['close'] > line.iloc[j - 1]['close'] else 0 for j in range(start_len, line_len)]),
            ])
            self.minTrendSpot = line.iloc[start_len:line_len]['high'].max() if trend >= (count_num*2-1) else 0

    # 设置大级别趋势
    def setMaxTrend(self):
        # self.meanLineTrend()
        self.nowPriceTrend()
        self.maxRunTimeTrend()

    # 均线涨跌时间判断趋势
    def meanLineTrend(self):
        if self.maxTrend > 0:
            return
        trend_day = 30
        trend_point = 0.618
        trendData = self.lines[self.MAX_TREND_INDEX]
        if self.sign == 1:
            # 30均线涨跌时间
            low_mean = [trendData['low'][i - trend_day:i].mean() if i >= trend_day else 0 for i in
                        range(0, len(trendData))]
            mean_len = len(low_mean) - trend_day
            up = sum([1 if low_mean[j] >= low_mean[j - 1] else 0 for j in range(trend_day, mean_len)])
            if up / mean_len > trend_point:
                self.maxTrend = 1
        elif self.sign == 2:
            high_mean = [trendData['high'][i - trend_day:i].mean() if i >= trend_day else 0 for i in
                         range(0, len(trendData))]
            mean_len = len(high_mean) - trend_day
            down = sum([1 if high_mean[j] <= high_mean[j - 1] else 0 for j in range(trend_day, mean_len)])
            if down / mean_len > trend_point:
                self.maxTrend = 2

    # 高低运行时间趋势
    def maxRunTimeTrend(self):
        if self.maxTrend > 0:
            return

        trend_point = 0.618
        trendData = self.lines[self.MAX_TREND_INDEX]

        high_idx = trendData.index.get_loc(trendData.loc[trendData['high'] == self.maxPrice].index[0])
        low_idx = trendData.index.get_loc(trendData.loc[trendData['low'] == self.minPrice].index[0])
        last_idx = trendData.index.get_loc(trendData.index[-1])

        up_down = (high_idx - low_idx) / last_idx
        if up_down > trend_point:
            self.maxTrend = 1
        elif up_down < -trend_point:
            self.maxTrend = 2

    # 当前价格趋势
    def nowPriceTrend(self):
        if self.maxTrend > 0:
            return

        trend_point = 0.618
        trendData = self.lines[self.MAX_TREND_INDEX]

        # 当前收线价格位置
        diff_price = (self.maxPrice - self.minPrice) * trend_point
        lastPrice = trendData.loc[trendData.index[-1], 'close']
        if lastPrice > self.minPrice + diff_price:
            self.maxTrend = 1
        elif lastPrice < self.maxPrice - diff_price:
            self.maxTrend = 2

    # 设置关键价格
    def setKeyPrice(self):
        mainLine = self.lines[self.PRICE_TREND]
        maxLine = self.lines[self.MAX_TREND_INDEX]

        h_high = mainLine.loc[mainLine['best'] > 0]['high'].values
        h_low = mainLine.loc[mainLine['worst'] > 0]['low'].values
        d_high = maxLine.loc[maxLine['best'] > 0]['high'].values
        d_low = maxLine.loc[maxLine['worst'] > 0]['low'].values

        all_plot = list(h_high) + list(h_low) + list(d_high) + list(d_low)

        self.maxPrice = max(d_high)
        self.minPrice = min(d_low)

        plot_rate = [-0.618, -0.5, -0.382, 0.382, 0.5, 0.618, 1]
        price_list = [j for j in all_plot]
        price_list += [i * (1 + j) for i in [self.minPrice, self.maxPrice, d_high[-1], d_low[-1]] for j in plot_rate]

        price_list.sort()
        price_line = []
        # 关键位具体价格
        self.keyPrice['price'] = {}
        for i in price_list:
            i_line = self.getLine(i)
            price_line.append(i_line)
            if i in all_plot:
                if i_line not in self.keyPrice['price']:
                    self.keyPrice['price'][i_line] = []
                self.keyPrice['price'][i_line].append(i)

        # 信号线收盘
        close_price = self.signLine['close'] if self.signLine else self.lines[self.KLINE_LIST[0]]['open'][-1]
        # 关键位 todo 开盘在关键位忽略 另行操作突破
        self.keyPrice['up'] = [i for i in price_line if i > close_price]
        self.keyPrice['down'] = [i for i in price_line if close_price > i]

        # todo 近期高低点的江恩位

    # 检测主要信号
    def checkMainSign(self):
        line = self.signLine
        # 上升卖空
        if self.sign == 2:
            high_line = self.getLine(line['high'])
            # 主指标设置
            if high_line >= self.keyPrice['up'][0]:
                self.mainSign['keyLine'] = 1
            else:
                raise ValueError('not in key price')
            # 是否关键位高低点
            if 0 < self.minTrendSpot <= line['high']:
                self.mainSign['trendSpot'] = 1
            else:
                raise ValueError('not trendSpot')
        # 下降时
        elif self.sign == 1:
            low_line = self.getLine(line['low'])
            # 主指标设置
            if low_line <= self.keyPrice['down'][-1]:
                self.mainSign['keyLine'] = 1
            else:
                raise ValueError('not in key price')
            # 是否关键位高低点
            if line['low'] <= self.minTrendSpot:
                self.mainSign['trendSpot'] = 1
            else:
                raise ValueError('not trendSpot')

    # 检测辅助信号
    def checkLessSign(self):
        ind = self.signInfo['date']
        num = self.signInfo['num']
        line = self.signLine
        dataLine = self.lines[ind]

        # 实体被包含
        if dataLine['high'][-(num + 1)] >= line['open'] and dataLine['low'][-(num + 1)] <= line['close']:
            self.lessSign['noEntity'] = 1

        # 线是否明显
        lastLen = line['high'] - line['low']
        show = sum([1 if lastLen > dataLine['high'][-i] - dataLine['low'][-i] else 0 for i in
                    range((num + 1), self.TREND_RANGE[ind] + (num + 1))])
        if show >= self.HIGH_RANGE[ind]:
            self.lessSign['bigLine'] = 1

        # 符合大趋势
        self.lessSign['inTrend'] = 1 if self.maxTrend == self.sign else 0

        # 上升卖空 假突破
        if self.sign == 2:
            high_line = self.getLine(line['high'])
            # 假突破
            max_price = max(self.keyPrice['price'][high_line]) if high_line in self.keyPrice['price'] else high_line
            self.lessSign['failBreak'] = 1 if line['high'] > max_price else 0
        # 下降时
        elif self.sign == 1:
            low_line = self.getLine(line['low'])
            # 假突破
            min_price = min(self.keyPrice['price'][low_line]) if low_line in self.keyPrice['price'] else low_line
            self.lessSign['failBreak'] = 1 if min_price > line['low'] else 0

    # 设置盈亏比
    def profitLoss(self):
        line = self.signLine
        line_len = line['high'] - line['low']
        # 开仓及止损
        order_stop = ['0:1', '0:0.618', '0:0.5', '0.382:0.618']
        profit_price = 0
        order_price = 0
        stop_price = -1
        for i in order_stop:
            order, stop = i.split(':')
            if self.sign == 1:
                profit_price = self.keyPrice['up'][0]
                order_price = float(line['high']) - float(order) * line_len
                stop_price = line['high'] - float(stop) * line_len
            elif self.sign == 2:
                profit_price = self.keyPrice['down'][-1]
                order_price = line['low'] + float(order) * line_len
                stop_price = line['low'] + float(stop) * line_len

            if (profit_price - order_price) / (order_price - stop_price) >= self.LOSS_RATE:
                self.lessSign['profitRate'] = 1
                self.signInfo['profit_price'] = profit_price
                self.signInfo['order_price'] = order_price
                self.signInfo['stop_price'] = stop_price
                break
            self.signInfo['profit_price'] = profit_price
            self.signInfo['order_price'] = order_price
            self.signInfo['stop_price'] = stop_price

    # 确认信号
    def confirmSign(self):
        less_sign = sum([1 if self.lessSign[v] > 0 else 0 for v in self.lessSign])
        if less_sign < 2:
            raise ValueError('less_sign no pass')

        if not self.invalidSign(self.signInfo):
            raise ValueError('now price is bad')

    # 无效信号
    def invalidSign(self, signInfo):
        if signInfo['sign'] < 1:
            return True

        if signInfo['sign'] == 1 and (self.data['market']['lastPrice'] <= signInfo['stop_price'] or self.data['market']['lastPrice'] >= signInfo['profit_price']):
            return False

        if signInfo['sign'] == 2 and (self.data['market']['lastPrice'] >= signInfo['stop_price'] or self.data['market']['lastPrice'] <= signInfo['profit_price']):
            return False

        return True

    # 获取水平画线
    def getLine(self, price):
        rd = self.PRICE_RANGE
        if price < 1000:
            rd = rd/10
        return int((price - 1 + rd / 2) / rd) * rd

    # 设置信号
    def getSign(self):
        self.singleSign(self.MAIN_TREND)
        self.doubleSign(self.MAIN_TREND)
        self.threeSign(self.MAIN_TREND)

        if self.WATCH_TREND:
            self.doubleSign(self.WATCH_TREND)
            self.threeSign(self.WATCH_TREND)

    # 单根信号: 锤 倒锤
    def singleSign(self, date):
        if self.sign > 0:
            return
        # 当前上一根
        num = 2
        line = self.lines[date].iloc[-num].to_dict()
        sign = self.shadowSign(line)
        self.setSign(sign, date, num, line)

    # 双根信号: 吞没
    def doubleSign(self, date):
        if self.sign > 0:
            return
        num = 3
        lineData = self.lines[date]

        leftLine = lineData.iloc[-num]
        rightLine = lineData.iloc[-(num - 1)]
        if not self.checkEngulf(leftLine, rightLine):
            return

        line = self.mergeKline(lineData[-num:-1])
        sign = self.shadowSign(line)
        self.setSign(sign, date, num, line)

    # 三根信号: 启明和黄昏
    def threeSign(self, date):
        if self.sign > 0:
            return
        num = 4
        lineData = self.lines[date]

        if not self.checkEngulf(lineData.iloc[-num], lineData.iloc[-(num - 2)]):
            return

        if not self.checkCross(lineData.iloc[-(num - 1)]):
            return

        line = self.mergeKline(lineData[-num:-1])
        sign = self.shadowSign(line)
        self.setSign(sign, date, num, line)

    # 检查影线信号
    def shadowSign(self, line):
        last_len = line['high'] - line['low']
        up_shadow = line['high'] - max(line['open'], line['close'])
        down_shadow = min(line['open'], line['close']) - line['low']
        if down_shadow * 3 > last_len * 2:
            return 1
        elif up_shadow * 3 > last_len * 2:
            return 2
        return -1

    # 检查吞没状态
    def checkEngulf(self, leftLine, rightLine):
        left_len = leftLine['high'] - leftLine['low']
        leftEntity = abs(leftLine['open'] - leftLine['close'])
        if leftEntity * 3 <= left_len * 2:
            return False
        right_len = rightLine['high'] - rightLine['low']
        rightEntity = abs(rightLine['open'] - rightLine['close'])
        if rightEntity * 3 <= right_len * 2:
            return False
        if leftEntity > rightEntity:
            if rightEntity * 3 < 2 * leftEntity:
                return False
        else:
            if leftEntity * 3 < 2 * rightEntity:
                return False
        return True

    # 检查十字星
    def checkCross(self, line):
        line_len = line['high'] - line['low']
        entity_len = abs(line['open'] - line['close'])
        if entity_len * 5 > line_len:
            return False
        return True

    # 合并k线
    def mergeKline(self, lineData):
        return {
            'timestamp': lineData['timestamp'][-1],
            'open': lineData['open'][0],
            'high': lineData['high'].max(),
            'low': lineData['high'].min(),
            'close': lineData['close'][-1],
            'volume': lineData['volume'][-1],
        }

    # 设置信号
    def setSign(self, sign, date, num, line):
        if sign <= 0:
            return
        self.sign = sign
        self.signInfo = {'sign': sign, 'date': date, 'num': num}
        self.signLine = line

    # 信号过期清空订单
    def clearSign(self):
        order = self.pri_redis.hgetall('order:parbin')
        signInfo = json.loads(order['signInfo']) if 'signInfo' in order else {'sign': -1}
        if self.sign <= 0:
            if not self.invalidSign(signInfo):
                self.pri_redis.hdelete('order:parbin')
                raise ValueError('invalidSign')
            return

        signLine = json.loads(order['signLine']) if 'signLine' in order else {'timestamp': 0}
        # 同一信号线已下单不再处理
        if signLine['timestamp'] == self.signLine['timestamp']:
            self.sign = -1
        # 上次信号过期 清除仓位订单
        elif self.data['position']['num'] != 0 or len(self.data['order']) > 0:
            self.pri_redis.hdelete('order:parbin')
            self.sign = 0

    # 下单
    def signOrder(self):
        if self.sign == 0:
            self.setOrder(self.sign)
        elif self.sign in [1, 2]:
            size = self.size
            open_order = {'symbol': self.symbol, 'price': self.signInfo['order_price'], 'orderQty': size,
                          'side': "Buy" if self.sign == 1 else "Sell"}
            profit_order = {'symbol': self.symbol, 'price': self.signInfo['profit_price'], 'orderQty': size,
                            'side': "Sell" if self.sign == 1 else "Buy", 'execInst': "ReduceOnly"}
            stop_order = {'symbol': self.symbol, 'stopPx': self.signInfo['stop_price'], 'orderQty': size,
                          'side': "Sell" if self.sign == 1 else "Buy", 'execInst': "ReduceOnly"}
            self.order['create'] = [open_order, profit_order, stop_order]
        self.order['time'] = self.signLine['timestamp']
        self.order['signLine'] = self.signLine
        self.order['signInfo'] = self.signInfo
        self.order['mainSign'] = self.mainSign
        self.order['lessSign'] = self.lessSign
        self.formatOrder()

    # 关键价格提醒
    def alertKeyPrice(self):
        nTime = time.strftime("%M:%S", time.localtime())

        # len(USER_CONFIG) 一个1秒
        if nTime[0:4] == '00:0' and int(nTime[4:5]) < 2:
            self.setBestWorst()
            self.setKeyPrice()
            self.setMaxTrend()
            self.ddAlert()
            raise ValueError('alertKeyPrice')

    # 发送
    def ddAlert(self):
        price = self.keyPrice.copy()
        ddData = {}
        if len(price['up']) > 0:
            ddData['上压'] = [price['price'][i] if i in price['price'] else i for i in price['up']]
        if len(price['down']) > 0:
            ddData['下压'] = [price['price'][price['down'][-i]] if price['down'][-i] in price['price'] else price['down'][-i] for i in range(1, (len(price['down'])+1))]
        ddData['趋势'] = self.maxTrend
        ddData['仓位'] = self.data['position']['num']
        ddData['总资产'] = self.data['margin']['allCost']
        ddData['订单保证金'] = self.data['margin']['orderCost']
        ddData['仓位保证金'] = self.data['margin']['posCost']
        ddData['最新价'] = self.data['market']['lastPrice']
        if 'create' in self.order:
            ddData['create'] = self.order['create']
        if 'delete' in self.order:
            ddData['delete'] = self.order['delete']
        if 'update' in self.order:
            ddData['update'] = self.order['update']
        self.dd.data(ddData)

    # 获取订单
    def getOrder(self):
        try:
            # 设置所需k线
            self.setStrategyKline()
            self.alertKeyPrice()
            # 获取信号
            self.getSign()
            # 过期信号及重复信号删除
            self.clearSign()
        except ValueError as e:
            return self.order

        # 验证信号
        if self.sign > 0:
            try:
                self.setBestWorst()
                self.setKeyPrice()
                # 获取一个趋势极点
                self.setMinTrend()
                self.checkMainSign()
                self.setMaxTrend()
                self.profitLoss()
                self.checkLessSign()
                self.confirmSign()
            except ValueError:
                # t, v, tb = sys.exc_info()
                # text = "".join(
                #     traceback.format_exception(t, v, tb)
                # )
                # print(text)
                self.sign = -1
        # 无效信号被重置
        self.signInfo['sign'] = self.sign

        if self.sign > -1:
            # 信号下单
            self.signOrder()
            # 记录信号订单
            self.pri_redis.hmset('order:parbin', self.order)
            self.ddAlert()
            logging.info({'id': self.runId, 'title': '策略完成', 'order': self.order, 'data': self.data})

        return self.order
