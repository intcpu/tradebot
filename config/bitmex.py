# encoding: utf-8
# https://www.bitmex.com/api/v1/instrument/activeIntervals


BITMEX = {
    'name': 'bitmex',  # 交易所名字
    'symbol': 'XBTUSD',  # 下单交易对
    'test':
        {
            'uid': 1,
            'name': 'bm:test_name',
            'key': '',
            'secret': '',
            'test_net': False,
            'redis_pri': 'tb2:bitmex:test1',
        },
    'test2':
        {
            'uid': 2,
            'name': 'bm:test2',
            'key': '',
            'secret': '',
            'test_net': True,
            'redis_pri': 'tb2:bitmex:test2',
        },
}


def getBmSymbol():
    import json, requests
    try:
        bm_proxies = {
            "https": "http://127.0.0.1:1087",
        }
        bm_symbol_request = requests.get('https://www.bitmex.com/api/v1/instrument/activeIntervals', timeout=15)

        symbols = json.loads(bm_symbol_request.text)['symbols']
    except:
        symbols = ['XBTUSD', 'ETHUSD', 'BCHUSD', 'XRPUSD', 'XBTZ20', 'XBTU20']

    return symbols


BM_ALL_SYMBOLS = getBmSymbol()
