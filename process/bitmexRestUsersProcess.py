# encoding: utf-8

import json
import logging
import sys
import time
import traceback

from api.bitmexRequest import bitmexRequest
from api.dingApi import dingApi
from client.redisClient import redisClient
from config.bitmex import BITMEX
from config.public import EVN, EVN_LIST
from config.user import USER_CONFIG
from manage.bitmexManage import bitmexManage
from master import master
from process.processInterface import processInterface


class bitmexRest:
    TRUST = 'trust'
    IS_TRUST = 'isTrust'

    def __init__(self, user=BITMEX['test'], sandbox=1):
        """
        每次只允许一个请求
        """

        self.name = user['name']
        self.user = user
        self.sandbox = sandbox
        self.api_key = user['key']
        self.api_secret = user['secret']
        self.test_net = user['test_net']
        self.session_num = 3

        self.orders = {}

        # 锁
        self.is_lock = 0
        # 最小执行锁
        self.min_lock = 1
        # 最后完成时间
        self.last_time = master.mictime()
        # 连续错误次数
        self.error_time = 0
        # 最后错误信息
        self.last_error = ''
        # 最后错误码
        self.last_code = 0

        self.dd = None
        self.bmxRest = None
        self.pub_redis = None
        self.redis = None

    def checkRest(self):
        try:
            # 模拟环境不开启请求
            if self.sandbox > 0:
                return

            if self.is_lock != 0:
                return
            if self.error_time > 2:
                master.sysError()
                master.kill_me()
            elif self.last_error:
                self.dd is not None and self.dd.msg(self.last_error)
                self.last_error = ''

            orders = self.redis.hgetall(self.TRUST)
            status = self.redis.get(self.IS_TRUST)

            # 0准备下单 1下单到用户进程 2远程下单 3下单失败
            if status == '1':
                logging.info({'title': 'bitmexRestLog status 1', 'self.last_time': self.last_time, 'self.orders': self.orders,
                              'status': status, 'orders': orders})
                if not orders or 'time' not in orders:
                    self.log('time not in orders')
                    return

                if self.last_time >= int(orders['time']):
                    self.log('status 1 stop: last_time: {} orders[time]: {}'.format(
                            self.last_time, orders['time']))
                    return

                # self.log('status 1 start')
                self.lock()
                self.last_time = int(orders['time'])
                self.redis.set(self.IS_TRUST, 2)
                self.orders = orders
                self.pushOrder()
                self.unlock()
                # self.log('status 1 end')

            elif status == '2':
                # self.log('status 2 start')
                if len(self.orders) > 2:
                    logging.info({'title': 'bitmexRestLog status 2', 'self.last_time': self.last_time,
                                  'self.orders': self.orders,
                                  'status': status, 'orders': orders})
                    if self.last_code == 400 or self.last_time - int(orders['time']) > 30000000:
                        for key in self.orders.keys():
                            self.log('order fail : del order key {}'.format(key))
                            # 失败订单不在重试
                            self.redis.hdel(self.TRUST, key)
                        self.log('status 2 del key end')
                        self.redis.set(self.IS_TRUST, 0)
                    else:
                        self.lock()
                        self.pushOrder()
                        self.unlock()
                else:
                    self.redis.set(self.IS_TRUST, 0)
                # self.log('status 2 end')
        except:

            t, v, tb = sys.exc_info()
            text = "".join(
                traceback.format_exception(t, v, tb)
            )
            self.log(text)
            self.unlock()
            master.sysError()

    def start(self):
        self.bmxRest = bitmexRequest()
        self.bmxRest.on_success = self.on_success
        self.bmxRest.on_send_order = self.on_send_order
        self.bmxRest.on_up_order = self.on_up_order
        self.bmxRest.on_cancel_order = self.on_cancel_order
        self.bmxRest.on_failed = self.on_failed
        self.bmxRest.on_error = self.on_error
        self.bmxRest.connect(self.api_key, self.api_secret, self.session_num, self.test_net, EVN['proxy_host'],
                             EVN['proxy_port'])

    def lock(self):
        self.is_lock += 1
        return True

    def unlock(self):
        if self.is_lock > 0:
            self.is_lock -= 1
        if self.is_lock == self.min_lock:
            self.set_time()
        return True

    def set_time(self):
        self.last_time = master.mictime()

    def reset_error(self, msg):
        self.log(msg)
        self.unlock()
        if self.is_lock == self.min_lock:
            self.error_time = 0
            self.last_code = 0
            self.last_error = ''

    def on_send_order(self, orders, request):
        if 'create' in self.orders:
            del self.orders['create']
        self.reset_error('添加订单成功')

    def on_up_order(self, orders, request):
        if 'update' in self.orders:
            del self.orders['update']
        self.reset_error('订单更新成功')

    def on_cancel_order(self, data, request):
        if 'delete' in self.orders:
            del self.orders['delete']
        self.reset_error('取消订单成功')

    def on_success(self, data, request):
        if request.path == '/trade/bucketed':
            key = 'kline{}'.format(request.params['binSize'])
            symbol = request.params['symbol']
            data.reverse()
            self.set_kline(key, symbol, data)
            self.log('{} success'.format(key))
            return
        self.reset_error('请求成功')

    def set_error(self, msg):
        self.log(msg)
        self.error_time += 1
        self.last_code = 0
        self.last_error = msg
        self.unlock()

    def on_failed(self, status_code, request):
        try:
            response = request.response.json()
            if response['error']['name'] == 'ValidationError':
                del self.orders['create']
        except:
            pass

        msg = '请求失败，状态码：{}，信息：{}'.format(status_code, request.response.text)
        self.log(msg)
        self.last_code = status_code
        self.last_error = msg
        self.unlock()

    def on_error(self, exception_type, exception_value, tb, request):
        # 有种情况 请求超时 但是已经到服务器了 会重复订单 不过下一次会删除
        self.set_error('请求异常，状态码：{}，信息：{}'.format(exception_type, exception_value))

    def init_kline(self, symbol=None, size=None):
        if not self.bmxRest:
            self.log('init_kline rest service is not start')
            return
        if not symbol or not size or len(size) == 0:
            self.log('init_kline error symbol: {} size: {}'.format(symbol, size))
            return

        try:
            redis = self.get_redis()
            for date in size:
                key = 'kline' + date
                redis.hdel(key, symbol)
                param = {'symbol': symbol, 'binSize': date}
                if date == '1d':
                    param['partial'] = True
                self.bmxRest.trade_data(param)
        except:
            t, v, tb = sys.exc_info()
            text = "".join(
                traceback.format_exception(t, v, tb)
            )
            self.log(text)

    # 设置k线
    def set_kline(self, key, symbol, data):
        redis = self.get_redis()
        kline = redis.hget(key, symbol)
        if kline is None:
            kline = data
        else:
            kline = json.loads(kline)
            if len(kline) > 0 and len(data) > 0:
                if kline[-1]['timestamp'] == data[0]['timestamp']:
                    del kline[-1]
            kline += data

        redis.hset(key, symbol, json.dumps(kline))

    # 设置k线
    def set_lv(self, symbol, lv):
        param = {'symbol': symbol, 'lv': lv}
        self.bmxRest.set_lv(param)

    # 获取公共redis
    def get_redis(self):
        if not self.pub_redis:
            evn_key = 'test' if self.user['test_net'] else 'line'
            redis_pub = EVN_LIST[evn_key][BITMEX['name']]
            self.pub_redis = redisClient(namespace=redis_pub)
        return self.pub_redis

    # 下单
    def pushOrder(self):
        if self.is_lock > self.min_lock or not self.bmxRest:
            return
        # self.log('pushOrder start')
        self.lock()
        logging.info(self.orders)
        if 'delete' in self.orders and self.orders['delete']:
            self.lock()
            self.bmxRest.del_order(json.loads(self.orders['delete']))
        if 'update' in self.orders and self.orders['update']:
            self.lock()
            self.bmxRest.up_order(json.loads(self.orders['update']))
        if 'create' in self.orders and self.orders['create']:
            self.lock()
            self.bmxRest.add_order(json.loads(self.orders['create']))
        self.unlock()
        # self.log('pushOrder end')

    def log(self, logStr):
        logging.info('user: {} is_lock: {} log: {}'.format(self.name, self.is_lock, logStr))


class bitmexRestUsersProcess(processInterface):
    PRO_CNAME = 'bitmexRest'

    def __init__(self):
        self.users = {}
        for conf in USER_CONFIG:
            if conf['site'] != BITMEX['name']:
                continue
            user = conf['user']
            uid = user['uid']

            self.users[uid] = bitmexRest(user, conf['sandbox'])

    def init(self):
        master.set_pro_name(self.PRO_CNAME)
        self.setConnect()

        while True:
            time.sleep(1)
            master.check_gid()
            for uid, rest in self.users.items():
                rest.checkRest()

    def setConnect(self):
        dd = dingApi()

        # 初始化连接 及 kline
        for conf in USER_CONFIG:
            uid = conf['user']['uid']
            if uid not in self.users:
                continue

            rest = self.users[uid]

            rest.dd = dd

            rest.redis = redisClient(namespace=conf['user']['redis_pri'])
            rest.redis.delete(rest.TRUST)
            rest.redis.set(rest.IS_TRUST, 0)

            rest.start()

            rest.init_kline(conf['symbol'], bitmexManage.KLINE_LIST)