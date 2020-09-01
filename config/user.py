# encoding: utf-8
# https://www.bitmex.com/api/v1/instrument/activeIntervals
from config.bitmex import BITMEX
from strategy.parbinStrategy import parbinStrategy
from strategy.shockStrategy import shockStrategy
from strategy.smaStrategy import smaStrategy

USER_CONFIG = [
    # {
    #     'site': 'bitmex',  # 站点名
    #     'sandbox': 0,  # 0真实环境 大于0沙盒模拟环境 不同值不同环境
    #     'user': BITMEX['test'],  # 站点用户
    #     'symbol': BITMEX['symbol'],  # 下单交易对
    #     'strategy': parbinStrategy,  # 策略方案
    #     'pub_sub': ['tradeBin1m', 'tradeBin5m', 'tradeBin1h', 'tradeBin1d', 'instrument'],  # wss 该站点公共数据
    #     'pri_sub': ['order', 'position', 'margin'],  # wss 用户数据
    # },
    {
        'site': 'bitmex',
        'sandbox': 0,
        'user': BITMEX['test'],
        'symbol': 'XBTUSD',
        'strategy': parbinStrategy,
        'pub_sub': ['tradeBin5m', 'tradeBin1h', 'tradeBin1d', 'instrument'],
        'pri_sub': ['order', 'position', 'margin'],
    },
    {
        'site': 'bitmex',
        'sandbox': 0,
        'user': BITMEX['test2'],
        'symbol': 'ETHUSD',
        'strategy': shockStrategy,
        'pri_sub': ['order', 'position', 'margin'],
    },
    {
        'site': 'bitmex',
        'sandbox': 1,
        'user': BITMEX['test'],
        'symbol': 'XBTUSD',
        'strategy': smaStrategy,
        'pri_sub': ['order', 'position', 'margin'],
    },
]
