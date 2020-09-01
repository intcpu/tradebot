# encoding: utf-8
import json

from market.baseMarket import basexMarket


class bitmexMarket(basexMarket):
    KEY = 'market'

    ORDERBOOK = 'orderBook10'
    FUNDING = 'funding'
    INSTRUMENT = 'instrument'
    TRADEBIN = 'tradeBin'
    KLINE = 'kline'

    MAX_LINE = 120

    orderbook_map = {
        'bids': 'buys',
        'asks': 'sells',
    }

    funding_map = {
        'fundingRate': 'nowFree',
        'fundingRateDaily': 'nextFree',
    }

    instrument_map = {
        'markPrice': 'markPrice',
        'indicativeSettlePrice': 'indexPrice',
        'fundingRate': 'nowFree',
        'indicativeFundingRate': 'nextFree',
        'bidPrice': 'latsBuy',
        'askPrice': 'latsSell',
        'lastPrice': 'lastPrice',
    }

    tradebin_map = {
        'symbol': 'symbol',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'volume': 'volume',
        'timestamp': 'timestamp',
    }

    def __init__(self):
        super().__init__()
        pass

    @staticmethod
    def get(redis, symbol):
        instrument = bitmexMarket.getInstrument(redis, symbol)
        # funding = bitmexMarket.getFunding(redis, symbol)
        # orderbook = bitmexMarket.getOrderbook(redis, symbol)

        data = {}

        data.update(instrument)
        # data.update(funding)
        # data.update(orderbook)

        return data

    @staticmethod
    def getOrderbook(redis, symbol):
        data = redis.hget(bitmexMarket.ORDERBOOK, symbol)
        jsonData = json.loads(data) if data else None
        jsonData = jsonData[0] if jsonData else {}

        mapData = {}
        for key, val in bitmexMarket.orderbook_map.items():
            mapData[val] = jsonData[key] if key in jsonData else []

        mapData['latsBuy'] = mapData['buys'][0][0] if 'buys' in mapData else 0
        mapData['latsSell'] = mapData['sells'][0][0] if 'sells' in mapData else 0

        return mapData

    @staticmethod
    def getFunding(redis, symbol):
        data = redis.hget(bitmexMarket.FUNDING, symbol)
        jsonData = json.loads(data) if data else None
        jsonData = jsonData[0] if jsonData else {}

        mapData = {}
        for key, val in bitmexMarket.funding_map.items():
            mapData[val] = jsonData[key] if key in jsonData else 0

        return mapData

    @staticmethod
    def getInstrument(redis, symbol):
        data = redis.hget(bitmexMarket.INSTRUMENT, symbol)
        jsonData = json.loads(data) if data else None
        jsonData = jsonData[0] if jsonData else {}

        mapData = {}
        for key, val in bitmexMarket.instrument_map.items():
            mapData[val] = jsonData[key] if key in jsonData else 0

        return mapData

    @staticmethod
    def kline(redis, date, symbol):
        klineKey = bitmexMarket.KLINE + date
        klineData = redis.hget(klineKey, symbol)
        klineJson = json.loads(klineData) if klineData else []

        if not klineJson:
            return []

        tradeKey = bitmexMarket.TRADEBIN + date
        tradeData = redis.hget(tradeKey, symbol)
        tradeJson = json.loads(tradeData) if tradeData else []

        jsonData = bitmexMarket.syncKline(klineJson, tradeJson)

        redis.hset(klineKey, symbol, json.dumps(jsonData))

        return jsonData

    @staticmethod
    def syncKline(klineJson, tradeJson):
        if len(klineJson) == 0 or len(tradeJson) == 0:
            jsonData = klineJson + tradeJson
        elif klineJson[-1]['timestamp'] == tradeJson[-1]['timestamp']:
            klineJson[-1] = tradeJson[-1]
            jsonData = klineJson
        elif len(tradeJson) >= bitmexMarket.MAX_LINE:
            jsonData = tradeJson
        else:
            timestamp = tradeJson[0]['timestamp']
            preKline = []
            for i in range(len(klineJson)):
                if timestamp == klineJson[i]['timestamp']:
                    break
                else:
                    preKline.append(klineJson[i])
            jsonData = preKline + tradeJson
        jsonData = jsonData[-bitmexMarket.MAX_LINE:]
        return jsonData