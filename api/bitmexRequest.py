# encoding: UTF-8
import os, sys
import json
import time
import logging
import hashlib, hmac
from urllib.parse import urlencode
from datetime import datetime
from threading import Lock
from client.restClient import restClient
from config.bitmex import BITMEX

REST_HOST = 'https://www.bitmex.com/api/v1'
TESTNET_REST_HOST = 'https://testnet.bitmex.com/api/v1'

WEBSOCKET_HOST = 'wss://www.bitmex.com/realtime'
TESTNET_WEBSOCKET_HOST = 'wss://testnet.bitmex.com/realtime'
# https://www.bitmex.com/api/explorer/swagger.json


class bitmexRequest(restClient):
    """
    BitMEX REST API
    """

    def __init__(self):
        """"""
        super().__init__()

        self.key = ""
        self.secret = ""

        self.order_pre = "api-"
        self.order_count = 100_000
        self.order_count_lock = Lock()

        self.connect_time = 0

    def sign(self, request):
        """
        Generate BitMEX signature.
        """
        # Sign
        expires = int(time.time() + 60)

        if request.params:
            query = urlencode(request.params)
            path = request.path + "?" + query
        else:
            path = request.path
        if request.data:
            request.data = urlencode(request.data)
        else:
            request.data = ""

        msg = request.method + "/api/v1" + path + str(expires) + request.data
        signature = hmac.new(
            self.secret, msg.encode(), digestmod=hashlib.sha256
        ).hexdigest()

        # Add headers
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "Connection": "close",
            "api-key": self.key,
            "api-expires": str(expires),
            "api-signature": signature,
        }
        request.headers = headers
        return request

    def connect(
            self,
            key: str,
            secret: str,
            session_num: int = 3,
            test_server: str = None,
            proxy_host: str = None,
            proxy_port: int = None,
    ):
        """
        Initialize connection to REST server.
        """
        self.key = key
        self.secret = secret.encode()

        self.connect_time = (
                int(datetime.now().strftime("%y%m%d%H%M")) * self.order_count
        )

        if test_server:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port)
        else:
            self.init(REST_HOST, proxy_host, proxy_port)

        self.start(session_num)

        logging.info("REST TRUE")

    def _new_order_id(self):
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    # ----------------------------------------------------------------------
    # ordType : Market(市价), Limit(限价), Stop(市价止损), StopLimit(限价止损), MarketIfTouched(市价止盈), LimitIfTouched(限价止盈)
    # displayQty: 显示数量(表示冰山委托 ordType为Limit)
    # execInst: ParticipateDoNotInitiate(被动委托),MarkPrice(标记价), IndexPrice(指数价), LastPrice(最新价), Close(触发后平仓,止盈止损使用), ReduceOnly(仅减仓,限价单使用)
    # timeInForce: GoodTillCancel(默认,一直有效,直到取消) ImmediateOrCancel(立即执行或取消) FillOrKill(全部成交或取消)

    # 限价单 {"ordType":"Limit","price":11568.5,"orderQty":495,"side":"Sell","execInst":"ParticipateDoNotInitiate,ReduceOnly","symbol":"XBTUSD","text":"Submission from testnet.bitmex.com"}
    # 市价单 {"ordType":"Market","orderQty":508,"side":"Buy","symbol":"XBTUSD","text":"Submission from testnet.bitmex.com"}
    # 当前价 11500
    # 指数触发买入止损,只平仓{"ordType":"Stop","stopPx":11000,"orderQty":495,"side":"Sell","execInst":"Close,IndexPrice","symbol":"XBTUSD","text":"Submission from testnet.bitmex.com"}
    # 买入止损 触发价高于当前价  卖出止损 触发价低于当前价
    # 指数触发止盈卖出,只平仓{"ordType":"MarketIfTouched","stopPx":12000,"orderQty":495,"side":"Sell","execInst":"Close,IndexPrice","symbol":"XBTUSD","text":"Submission from testnet.bitmex.com"}
    # 止盈买入 触发价低于当前价  止盈卖出 触发价高于当前价(与限价单相似)
    # 追踪止损 pegOffsetValue追踪价距 >0买入 <0卖出 {"ordType":"Stop","pegOffsetValue":1,"pegPriceType":"TrailingStopPeg","orderQty":30,"side":"Buy","execInst":"LastPrice","symbol":"XBTUSD","text":"Submission from testnet.bitmex.com"}
    # 限价止损 {"ordType":"StopLimit","price":12000,"stopPx":12000,"orderQty":30,"side":"Buy","execInst":"LastPrice","symbol":"XBTUSD","text":"Submission from testnet.bitmex.com"}
    # 限价止盈 {"ordType":"LimitIfTouched","price":1000,"stopPx":12000,"orderQty":30,"side":"Sell","execInst":"LastPrice","symbol":"XBTUSD","text":"Submission from testnet.bitmex.com"}
    # 限价止盈止损都是以触发价与当前触发价格类型对比
    def send_order(self, orderReq):  # type: (VtOrderReq)->str
        """"""
        orderid = self.order_pre + str(self.connect_time + self._new_order_id())

        data = {
            'symbol': orderReq['symbol'],
            # 'side': orderReq.side,
            'orderQty': int(orderReq['orderQty']),
            'clOrdID': orderid,
            'text': 'tradebot',
        }

        if orderReq.get('price') and orderReq.get('stopPx'):
            data['ordType'] = 'StopLimit'
            data['price'] = orderReq['price']
            data['stopPx'] = orderReq['stopPx']
        elif orderReq.get('stopPx') and orderReq.get('price') is None:
            data['ordType'] = 'Stop'
            data['stopPx'] = orderReq['stopPx']
        elif orderReq.get('price'):
            data['ordType'] = 'Limit'
            data['price'] = orderReq['price']
        else:
            data['ordType'] = 'Market'

        if orderReq.get('side'):
            data['side'] = orderReq['side']

        if orderReq.get('execInst') is not None:
            data['execInst'] = orderReq['execInst']

        self.add_request('POST', '/order',
                         callback=self.on_send_order,
                         data=data,
                         on_failed=self.on_failed,
                         on_error=self.on_error)

        return orderid

    # 止盈订单
    def other_order(self, orderReq):
        """"""
        orderid = self.order_pre + str(self.connect_time + self._new_order_id())

        data = {
            'symbol': orderReq['symbol'],
            # 'side': orderReq.side,
            'orderQty': orderReq['orderQty'],
            'clOrdID': orderid,
            'execInst': orderReq.get('execInst', None),
            'text': 'tradebot',
        }

        if orderReq.get('price') and orderReq.get('stopPx'):
            data['ordType'] = 'LimitIfTouched'
            data['price'] = orderReq.price
            data['stopPx'] = orderReq.stopPx
        elif orderReq.get('stopPx') and orderReq.get('price') is None:
            data['ordType'] = 'MarketIfTouched'
            data['stopPx'] = orderReq.stopPx
        else:
            return False

        if orderReq.get('side'):
            data['side'] = orderReq['side']

        self.add_request('POST', '/order',
                         callback=self.on_send_order,
                         data=data,
                         on_failed=self.on_failed,
                         on_error=self.on_error)

        return orderid

    # 批量添加限价市价订单
    def add_order(self, orders):
        orderids = []
        datas = []
        for order in orders:
            orderid = self.order_pre + str(self.connect_time + self._new_order_id())

            data = {
                'symbol': order['symbol'],
                # 'side': orderReq.side,
                # 'orderQty': int(order['orderQty']),
                'clOrdID': orderid,
                'text': 'tradebot',
            }

            orderQty = int(order['orderQty'])
            if orderQty > 0:
                data['side'] = 'Buy'
                data['orderQty'] = orderQty
            else:
                data['side'] = 'Sell'
                data['orderQty'] = abs(orderQty)

            if order.get('side'):
                data['side'] = order['side']

            if order.get('price') and order.get('stopPx'):
                data['ordType'] = 'StopLimit'
                data['price'] = order['price']
                data['stopPx'] = order['stopPx']
            elif order.get('stopPx') and order.get('price') is None:
                data['ordType'] = 'Stop'
                data['stopPx'] = order['stopPx']
            elif order.get('price'):
                data['ordType'] = 'Limit'
                data['price'] = order['price']
            else:
                data['ordType'] = 'Market'

            if order.get('execInst') is not None:
                data['execInst'] = order['execInst']

            datas.append(data)
            orderids.append(orderid)

        params = {'orders': json.dumps(datas)}
        self.add_request('POST', '/order/bulk',
                         callback=self.on_send_order,
                         data=params,
                         on_failed=self.on_failed,
                         on_error=self.on_error
                         )

        return orderids

    # 批量更新订单
    def up_order(self, orders: list = None):
        if orders is None:
            return 'param error'

        datas = []
        for order in orders:
            data = {}
            for key, val in order.items():
                if key == 'orderID':
                    if order[key][0:len(self.order_pre)] == self.order_pre:
                        data['clOrdID'] = val
                    else:
                        data['orderID'] = val
                else:
                    data[key] = val
            datas.append(data)

        params = {'orders': json.dumps(datas)}
        self.add_request(
            "PUT",
            "/order/bulk",
            callback=self.on_up_order,
            data=params,
            on_failed=self.on_failed,
            on_error=self.on_error
        )

    # 批量取消订单
    def del_order(self, orders: list = None):
        if orders is None:
            return
        clOrdID = ''
        orderID = ''
        for order in orders:
            if order['orderID'][0:len(self.order_pre)] == self.order_pre:
                clOrdID += order['orderID'] + ','
            else:
                orderID += order['orderID'] + ','
        orderID = orderID.strip(',')
        clOrdID = clOrdID.strip(',')
        params = {'orderID': orderID, 'clOrdID': clOrdID}
        self.add_request(
            "DELETE",
            "/order",
            callback=self.on_cancel_order,
            params=params,
            on_failed=self.on_failed,
            on_error=self.on_error,
        )

    # 取消所有订单
    def cancel_all(self, symbol):
        """"""
        params = {'symbol': symbol}

        self.add_request(
            "DELETE",
            "/order/all",
            callback=self.on_cancel_order,
            params=params,
            on_failed=self.on_failed,
            on_error=self.on_error,
        )

    # 获取k线
    def trade_data(self, res: list = None):
        """"""
        params = {
            'symbol': res['symbol'],
            'binSize': res['binSize'],
            'columns': 'symbol,timestamp,open,high,low,close,volume',
            # 'partial':True,
            'reverse': True,
            'count': 120,
        }
        if 'partial' in res:
            params['partial'] = res['partial']
        if 'reverse' in res:
            params['reverse'] = res['reverse']
        if 'startTime' in res:
            params['startTime'] = res['startTime']
        if 'endTime' in res:
            params['endTime'] = res['endTime']

        self.add_request(
            "GET",
            "/trade/bucketed",
            callback=self.on_success,
            params=params,
            on_failed=self.on_failed,
            on_error=self.on_error,
        )

    # 设置杠杆
    def set_lv(self, res: list = None):
        """"""
        params = {
            'symbol': res['symbol'],
            'leverage': res['lv'],
        }

        self.add_request(
            "POST",
            "/position/leverage",
            callback=self.on_success,
            params=params,
            on_failed=self.on_failed,
            on_error=self.on_error,
        )

    # 获取所有合约
    # https://www.bitmex.com/api/v1/instrument/activeIntervals
    def all_xbt(self):
        """"""
        params = {}
        self.add_request(
            "GET",
            "instrument/activeIntervals",
            callback=self.on_success,
            params=params,
            on_failed=self.on_failed,
            on_error=self.on_error,
        )

    def on_send_order(self, data, request):
        """Websocket will push a new order status"""
        logging.info(data)

    def on_up_order(self, data, request):
        """Websocket will push a new order status"""
        logging.info(data)

    def on_cancel_order(self, data, request):
        """Websocket will push a new order status"""
        pass

    def on_success(self, data, request):
        logging.info(data)

    def on_failed(self, status_code, request):
        """
        Callback to handle request failed.
        """
        msg = f"请求失败，状态码：{status_code}，信息：{request.response.text}"
        logging.error(msg)

    def on_error(self, exception_type, exception_value, tb, request):
        """
        Callback to handler request exception.
        """
        logging.error(
            self.exception_detail(exception_type, exception_value, tb, request)
        )


if __name__ == '__main__':
    PROXY_HOST = '127.0.0.1'
    PROXY_PORT = 8118
    SYMBOL = 'XBTUSD'
    # ceshi
    API_KEY = BITMEX['test']['key']
    API_SECRET = BITMEX['test']['secret']

    bt = bitmexRequest()
    bt.connect(API_KEY, API_SECRET, 2, True, PROXY_HOST, PROXY_PORT)
    logging.basicConfig(filename="logs/log", filemode="w", format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                        level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S")
    bt.trade_data({'symbol': 'XBTUSD', 'binSize': '1d'})
    while True:
        # bt.send_order({'symbol':'XBTUSD','price':11400,'orderQty':500})
        time.sleep(10)
    pass
