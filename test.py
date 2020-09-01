# encoding: utf-8
import time, logging

from backtest.bitmexTestProcess import bitmexTestProcess

logging.basicConfig(
    filename="./logs/" + time.strftime("%Y%m%d") + '.log',
    filemode="a",
    format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
)

t = bitmexTestProcess()
t.init()
