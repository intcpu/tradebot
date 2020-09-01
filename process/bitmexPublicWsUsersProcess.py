# encoding: utf-8
import json
import logging
import traceback

from config.user import USER_CONFIG
from master import master
from api.bitmexWebsocket import bitmexWebsocket
from client.redisClient import redisClient
from config.bitmex import BITMEX
from config.public import EVN, EVN_LIST
from process.processInterface import processInterface


class bitmexPublicWs:

    def __init__(self, user=BITMEX['test'], symbol=BITMEX['symbol'], pub_sub=None):
        """"""
        self.user = user
        self.symbol = symbol
        self.pub_sub = pub_sub

        self.btWss = None
        self.redis = None

        self.uid = 0
        self.name = user['name']

    def link(self):
        self.btWss = bitmexWebsocket()
        redis_pub = EVN_LIST['test'][BITMEX['name']] if self.user['test_net'] else EVN_LIST['line'][BITMEX['name']]
        self.redis = redisClient(namespace=redis_pub)
        self.btWss.public_subs = self.pub_sub

        # self.btWss.public_subs = ['trade', 'tradeBin1m', 'tradeBin5m', 'tradeBin1h', 'tradeBin1d', 'orderBook10', 'funding']
        self.btWss.private_subs = []
        # self.btWss.callback_dict = {'tradeBin1m':formatData.tradeBin1m}
        self.btWss.on_error = self.on_error_ws
        self.btWss.on_disconnected = self.on_disconnected
        self.btWss.on_action = self.wss_action

        self.btWss.connect(symbols=self.symbol, test_net=self.user['test_net'], proxy_host=EVN['proxy_host'], proxy_port=EVN['proxy_port'])

    def wss_action(self, action, table, symbol, data):
        master.check_gid()
        try:
            fullData = self.btWss.data[table][symbol] if table in self.btWss.data and symbol in self.btWss.data[table] else []
            self.redis.hset(table, self.symbol, json.dumps(fullData))
            self.redis.set('time', master.mictime())
        except:
            logging.info('{} pub wss try except'.format(self.name))
            master.sysError()

    def on_disconnected(self):
        """连接回调"""
        master.ding('{} pub wss 连接断开'.format(self.name))
        self.btWss.subscribe()
        self.btWss.authenticate()

    def on_error_ws(self, exception_type, exception_value, tb):
        text = "".join(
            traceback.format_exception(exception_type, exception_value, tb)
        )
        logging.error(text)
        master.ding('{} pub wss 进程终止'.format(self.name))
        master.kill_me()


class bitmexPublicWsUsersProcess(processInterface):
    PRO_CNAME = 'publicWss'

    def __init__(self):
        self.users = {}

        for conf in USER_CONFIG:
            if conf['site'] != BITMEX['name']:
                continue
            user = conf['user']
            uid = str(user['uid'])+conf['symbol']
            if 'pub_sub' in conf:
                self.users[uid] = bitmexPublicWs(user, conf['symbol'], conf['pub_sub'])

    def init(self):
        master.set_pro_name(self.PRO_CNAME)

        for uid, wss in self.users.items():
            wss.link()
