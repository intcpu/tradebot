# encoding: utf-8
import importlib
import logging
import multiprocessing as mp
import os
import signal
import sys
import time
import traceback
import setproctitle
from config.public import PROJECT_NAME, EVN


class master:
    PRO_CNAME = 'matser'

    def __init__(self):
        # 进程信息
        self.process = {}
        self.time = int(time.time())

        # 监控Process.py 重启自己  监控 Strategy.py Position.py Order.py 重启 strategyProcess.py
        self.process_paths = dict(process=['Process.py', None],
                                  strategy=['Strategy.py', 'strategyProcess.py'],
                                  position=['Position.py', 'strategyProcess.py'],
                                  order=['Order.py', 'strategyProcess.py'],
                                  market=['Market.py', 'strategyProcess.py'],
                                  margin=['Margin.py', 'strategyProcess.py'],
                                  manage=['Manage.py', 'strategyProcess.py'],
                                  sandbox=['Sandbox.py', 'strategyProcess.py'],
                                  )
        self.watched_mtimes = {}

    # run
    def run(self):
        master.set_pro_name(self.PRO_CNAME)
        self.set_mtimes()

        while True:
            try:
                self.check_mtimes()
                self.check_process()
                time.sleep(3)
            except:
                master.error_msg()
                master.kill_me()

    # 增加子进程
    def add_process(self, obj, param=[]):
        try:
            pname = obj.PRO_CNAME
            objClass = obj()
            if pname in self.process:
                raise Exception(str(os.getpid()) + '>> process ' + pname + ' has existed')
            else:
                process = mp.Process(target=objClass.init, args=param, name='py-service-' + pname)
                # process.daemon = True
                process.start()
                self.process[pname] = {'obj': obj, 'param': param, 'process': process}
                master.ding(str(os.getpid()) + '>> ' + pname + ' process is start')
        except:
            master.error_msg()

    # 检查所有子进程
    def check_process(self):
        try:
            for p, pv in self.process.items():
                if 'process' in pv:
                    process = pv['process']
                else:
                    continue
                if process and process.is_alive() is not True:
                    # process.terminate()
                    process.kill()
                    process = None
                    logging.info(str(os.getpid()) + '>> ' + p + ' process is_alive: false')

                if process is None:
                    objClass = pv['obj']()
                    process = mp.Process(target=objClass.init, args=pv['param'], name='py-service-' + p)
                    process.start()
                    self.process[p] = {'obj': pv['obj'], 'param': pv['param'], 'process': process}
                    master.ding(str(os.getpid()) + '>> ' + p + ' process is start')
        except:
            master.error_msg()

    # 检查进程模块是否被修改
    def check_mtimes(self):
        is_reload = False
        for name, file in self.watched_mtimes.items():
            for class_name, model_name, model_path, model_mtime in file:
                if os.path.getmtime(model_path) > model_mtime:
                    logging.info('{} is change'.format(model_name))
                    importlib.reload(importlib.import_module(model_name))
                    class_name, model_name, model_path, model_mtime = self.watched_mtimes[name][0]
                    logging.info('{} is reload'.format(model_name))
                    model_obj = importlib.reload(importlib.import_module(model_name))
                    model_class = getattr(model_obj, class_name)
                    process_name = getattr(model_class, 'PRO_CNAME')
                    if process_name in self.process:
                        self.process[process_name]['obj'] = model_class
                        self.process[process_name]['process'].kill()
                    is_reload = True
        if is_reload:
            self.set_mtimes()

    # 设置修改时间
    def set_mtimes(self):
        self.watched_mtimes = {}
        base_dir = os.getcwd() + os.sep
        for su, fa in self.process_paths.items():
            path = base_dir + su
            files = os.listdir(path)
            for f in files:
                # 文件名称不包含 fa[0] 不监控
                if fa[0] not in f: continue
                # 设置 fa[1] 则将fa[0]所有修改后 只重启加载fa[1]
                if fa[1]:
                    name = fa[1]
                    if name not in self.watched_mtimes: continue
                else:
                    name = f
                    self.watched_mtimes[name] = []
                self.watched_mtimes[name].append(
                    [f[0:-3], '{}.{}'.format(su, f[0:-3]), path + os.sep + f, os.path.getmtime(path + os.sep + f)])

    # 设置进程名字
    @staticmethod
    def set_pro_name(pro_name=''):
        pro_name = 'python3-{}-{}-{}'.format(PROJECT_NAME, EVN['api_name'], pro_name)
        setproctitle.setproctitle(pro_name)
        logging.info('{} process init'.format(pro_name))

    # 检查主进程
    @staticmethod
    def check_gid():
        pid = os.getpid()
        gid = os.getpgid(pid)
        try:
            os.kill(gid, 0)
        except OSError:
            msg = str(os.getpid()) + '>> gid:' + str(gid) + ' is killed, pid:' + str(pid) + ' will kill'
            master.ding(msg)
            os.kill(pid, signal.SIGKILL)
            return False
        else:
            return True

    # 系统错误
    @staticmethod
    def sysError():
        t, v, tb = sys.exc_info()
        logging.error(str(os.getpid()) + '>> manager process ' + str(t))
        if t in [ConnectionRefusedError, EOFError, BrokenPipeError]:
            master.stop()
        elif t not in [KeyboardInterrupt]:
            master.error_msg(t, v, tb)
            master.kill_me()

    # 进程自杀
    @staticmethod
    def kill_me():
        pid = os.getpid()
        master.ding(str(os.getpid()) + '>> pid ' + str(pid) + ' has be killed')
        os.kill(pid, signal.SIGKILL)

    # 脚本自杀
    @staticmethod
    def stop():
        # python = sys.executable
        # os.execl(python, python, *sys.argv)
        master.ding(str(os.getpid()) + '>> ------STOP-----')
        try:
            os.kill(os.getpgid(os.getpid()), signal.SIGKILL)
        except:
            master.sysError()

    # 报错信息
    @staticmethod
    def error_msg(t=None, v=None, tb=None):
        if not t:
            t, v, tb = sys.exc_info()
        text = "".join(
            traceback.format_exception(t, v, tb)
        )
        master.ding(text)

    @staticmethod
    def time():
        return int(time.time())

    @staticmethod
    def mictime():
        return int(time.time_ns()/1000)

    # 钉钉消息
    @staticmethod
    def ding(data):
        logging.info(data)
