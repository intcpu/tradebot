# encoding: utf-8
import json

from order.baseOrder import baseOrder


class bitmexOrder(baseOrder):
    KEY = 'order'
    key_map = {
        'symbol': 'symbol',
        'orderID': 'orderID',
        'side': 'side',
        'price': 'price',
        'orderQty': 'allNum',
        'stopPx': 'stopPx',
        'leavesQty': 'unDealNum',
        'cumQty': 'dealNum',
        'execInst': 'isReduce',
    }

    def __init__(self):
        super().__init__()
        pass

    @staticmethod
    def get(redis, symbol):
        data = redis.hget(bitmexOrder.KEY, symbol)
        jsonData = json.loads(data) if data else []

        orders = []
        for order in jsonData:
            mapData = {}
            for key, val in bitmexOrder.key_map.items():
                if key in ['execInst'] and key in order and type(order[key]) == str:
                    mapData[val] = 1 if order[key].find('ReduceOnly') >= 0 else 0
                else:
                    mapData[val] = order[key] if key in order else 0
            orders.append(mapData)

        return orders
