# encoding: utf-8
import json
import logging
import traceback

from api.bitmexWebsocket import bitmexWebsocket
from client.redisClient import redisClient
from config.bitmex import BITMEX
from config.public import EVN, EVN_LIST
from config.user import USER_CONFIG
from master import master
from process.processInterface import processInterface


class bitmexPrivateWs:
    IS_PUB_SUB = 0  # 允许订阅公共数据

    def __init__(self, user=BITMEX['test'], symbol=BITMEX['symbol'], pri_sub=None, pub_sub=None):
        """"""
        self.user = user
        self.symbol = symbol
        self.pri_sub = pri_sub
        self.pub_sub = pub_sub

        self.uid = 0
        self.name = user['name']
        self.btWss = None
        self.pri_redis = None
        self.pub_redis = None

    def link(self):
        self.pri_redis = redisClient(namespace=self.user['redis_pri'])

        redis_pub = EVN_LIST['test'][BITMEX['name']] if self.user['test_net'] else EVN_LIST['line'][BITMEX['name']]
        self.pub_redis = redisClient(namespace=redis_pub)

        self.btWss = bitmexWebsocket()
        # self.btWss.callback_dict = {'tradeBin1m':formatData.tradeBin1m}
        self.btWss.on_error = self.on_error_ws
        self.btWss.on_disconnected = self.on_disconnected
        self.btWss.on_action = self.wss_action

        if self.pri_sub is not None:
            self.uid = self.user['uid']
            self.btWss.private_subs = self.pri_sub

        if self.IS_PUB_SUB == 1 and self.pub_sub is not None:
            self.btWss.public_subs = self.pub_sub

        self.btWss.connect(symbols=self.symbol, api_key=self.user['key'], api_secret=self.user['secret'],
                           test_net=self.user['test_net'], proxy_host=EVN['proxy_host'],
                           proxy_port=EVN['proxy_port'])

    def wss_action(self, action, table, symbol, data):
        master.check_gid()
        try:
            fullData = self.btWss.data[table][symbol] if table in self.btWss.data and symbol in self.btWss.data[
                table] else []

            if table == 'margin':
                fullData = self.btWss.data[table] if table in self.btWss.data else {}
                self.pri_redis.set(table, json.dumps(fullData))
                self.pri_redis.set('time', master.mictime())
            elif table in self.pri_sub:
                self.pri_redis.hset(table, self.symbol, json.dumps(fullData))
                self.pri_redis.set('time', master.mictime())
            else:
                self.pri_redis.hset(table, self.symbol, json.dumps(fullData))
                self.pub_redis.set('time', master.mictime())
        except:
            logging.info('{} pri wss try except'.format(self.name))
            master.sysError()

    def on_disconnected(self):
        """连接回调"""
        master.ding('{} pri wss 连接断开'.format(self.name))
        self.btWss.subscribe()
        self.btWss.authenticate()

    def on_error_ws(self, exception_type, exception_value, tb):
        text = "".join(
            traceback.format_exception(exception_type, exception_value, tb)
        )
        logging.error(text)
        master.ding('{} pri wss 进程终止'.format(self.name))
        master.kill_me()


class bitmexPrivateWsUsersProcess(processInterface):
    PRO_CNAME = 'privateWss'

    def __init__(self):
        self.users = {}

        for conf in USER_CONFIG:
            if conf['site'] != BITMEX['name']:
                continue
            user = conf['user']
            uid = str(user['uid'])+conf['symbol']
            pri_sub = conf['pri_sub'] if 'pri_sub' in conf else None
            pub_sub = conf['pub_sub'] if 'pub_sub' in conf else None

            if pri_sub or pub_sub:
                self.users[uid] = bitmexPrivateWs(user, conf['symbol'], pri_sub, pub_sub)

    def init(self):
        master.set_pro_name(self.PRO_CNAME)

        for uid, wss in self.users.items():
            wss.link()
