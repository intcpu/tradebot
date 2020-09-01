# encoding: utf-8
import time, logging

from master import master
from process.bitmexPublicWsUsersProcess import bitmexPublicWsUsersProcess
from process.bitmexPrivateWsUsersProcess import bitmexPrivateWsUsersProcess
from process.strategyProcess import strategyProcess
from process.bitmexRestUsersProcess import bitmexRestUsersProcess

logging.basicConfig(
    filename="./logs/" + time.strftime("%Y%m%d") + '.log',
    filemode="a",
    format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
)

mat = master()

mat.add_process(bitmexPublicWsUsersProcess)
mat.add_process(bitmexPrivateWsUsersProcess)
mat.add_process(bitmexRestUsersProcess)
mat.add_process(strategyProcess)

mat.run()
