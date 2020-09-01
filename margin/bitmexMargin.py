# encoding: utf-8
import json

from margin.baseMargin import baseMargin


class bitmexMargin(baseMargin):
    KEY = 'margin'
    key_map = {
        'walletBalance': 'allCost',
        'initMargin': 'orderCost',
        'maintMargin': 'posCost',
        'availableMargin': 'useCost',
        'unrealisedPnl': 'unGet',
    }

    def __init__(self):
        super().__init__()
        pass

    @staticmethod
    def get(redis):
        data = redis.get(bitmexMargin.KEY)
        jsonData = json.loads(data) if data else {}
        mapData = {}
        for key, val in bitmexMargin.key_map.items():
            mapData[val] = jsonData[key] if key in jsonData else 0

        return mapData
