# encoding: utf-8
import abc


class baseSandbox(object):
    __metaclass__ = abc.ABCMeta

    MAPS = {}

    # 设置沙盒环境
    @abc.abstractmethod
    def setBox(self, **kwargs): pass

    # 运行沙盒环境
    @abc.abstractmethod
    def runBox(self, **kwargs): pass
