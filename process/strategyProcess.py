# encoding: utf-8
import importlib
import logging
import time
import config
from client.redisClient import redisClient
from master import master
from process.processInterface import processInterface
from config.public import EVN_LIST


class strategyProcess(processInterface):
    PRO_CNAME = 'strategy'
    TRUST = 'trust'
    IS_TRUST = 'isTrust'

    def __init__(self):
        """"""
        self.redis = {}
        self.manage = {}

    def initUser(self, userConfig):
        try:
            for conf in userConfig:
                user = conf['user']
                name = user['name']
                # 个人数据redis
                self.redis[name+str(conf['sandbox'])] = conf['strategy'].setRedis(conf)

                redis_pub = EVN_LIST['test' if user['test_net'] else 'line'][conf['site']]
                pub_key = conf['site'] + str(user['test_net'])

                if pub_key not in self.redis:
                    self.redis[pub_key] = redisClient(namespace=redis_pub)
        except:
            master.sysError()

    def init(self):
        master.set_pro_name(self.PRO_CNAME)

        importlib.reload(config.user)
        from config.user import USER_CONFIG

        self.initUser(USER_CONFIG)
        while True:
            try:
                time.sleep(1)
                master.check_gid()
                now_time = master.mictime()

                for conf in USER_CONFIG:
                    user = conf['user']
                    name = user['name']
                    symbol = conf['symbol']

                    pri_redis = self.redis[name+str(conf['sandbox'])]
                    pub_redis = self.redis[conf['site'] + str(user['test_net'])]

                    # 获取策略
                    strategy = conf['strategy'](user, pub_redis, pri_redis, symbol)
                    # 获取开仓订单
                    orders = strategy.getOrder()

                    # 沙盒环境
                    if conf['sandbox'] > 0:
                        strategy.runBox()
                        continue

                    status = pri_redis.get(self.IS_TRUST)

                    # 60秒仓位日志
                    if int(now_time/1000000) % 300 == 0:
                        logging.info({'title': 'strategyProcessLog', 'name': name, 'sign': strategy.sign, 'status': status, 'orders': orders, 'data': strategy.data})

                    # 用户订单仓位未更新
                    lastInfo = pri_redis.hget(self.TRUST, 'info')

                    if not orders or 'info' not in orders or lastInfo == orders['info']:
                        continue

                    # 正在下单不处理
                    if status in ['1', '2']:
                        continue
                    # 状态为0 新下单
                    elif status == '0' and len(orders) > 2:
                        logging.info({'title': 'strategyCreateOrder', 'name': name, 'sign': strategy.sign, 'status': status, 'orders': orders, 'data': strategy.data})
                        pri_redis.hdelete(self.TRUST)
                        pri_redis.hmset(self.TRUST, orders)
                        pri_redis.set(self.IS_TRUST, 1)
            except:
                master.sysError()

    # 获取用户最后更新时间
    def getUserTime(self, user_name):
        user_time = self.redis[user_name].get('time')
        return 0 if user_time is None else int(user_time)