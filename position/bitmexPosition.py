# encoding: utf-8
import json

from position.basePosition import basePosition


class bitmexPosition(basePosition):
    KEY = 'position'
    key_map = {
        'symbol': 'symbol',
        'avgEntryPrice': 'price',
        'currentQty': 'num',
        'maintMargin': 'margin',
        'homeNotional': 'cost',
        'liquidationPrice': 'liquiPrice',
        'unrealisedPnl': 'unGet',
        'unrealisedRoePcnt': 'unGetRate',
        'leverage': 'lv',
        'crossMargin': 'isFull',
    }

    def __init__(self):
        super().__init__()
        pass

    @staticmethod
    def get(redis, symbol):
        data = redis.hget(bitmexPosition.KEY, symbol)
        jsonData = json.loads(data) if data else None
        jsonData = jsonData[0] if jsonData else {}
        mapData = {}
        for key, val in bitmexPosition.key_map.items():
            mapData[val] = jsonData[key] if key in jsonData else 0
            if mapData[val] in [True, False, None]:
                mapData[val] = 1 if mapData[val] else 0

        return mapData

