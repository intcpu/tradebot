# encoding: utf-8
EVN_LIST = {
    'test':
        {
            'api_name': 'test',
            'proxy_host': '127.0.0.1',
            'proxy_port': '1087',
            'bitmex': 'tb2:bitmex:test',  # bitmex模拟盘公共数据
        },
    'line':
        {
            'api_name': 'line',
            'proxy_host': None,
            'proxy_port': None,
            'bitmex': 'tb2:bitmex:line',  # bitmex实盘公共数据
        }
}

PROJECT_NAME = 'tradeBot'
DING_TOKEN = ''
EVN = EVN_LIST['test']
