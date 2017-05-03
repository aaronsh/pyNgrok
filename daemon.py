#!/usr/bin/env python
# coding: utf-8

# python模拟linux的守护进程

import sys, os, time, atexit, string
from signal import SIGTERM

import common


class Daemon:
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        # 需要获取调试信息，改为stdin='/dev/stdin', stdout='/dev/stdout', stderr='/dev/stderr'，以root身份运行。
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self.procedure = self._run

    def _daemonize(self):
        try:
            pid = os.fork()  # 第一次fork，生成子进程，脱离父进程
            if pid > 0:
                sys.exit(0)  # 退出主进程
        except OSError, e:
            sys.stderr.write('fork #1 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)

        os.chdir("/")  # 修改工作目录
        os.setsid()  # 设置新的会话连接
        os.umask(0)  # 重新设置文件创建权限

        try:
            pid = os.fork()  # 第二次fork，禁止进程打开终端
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write('fork #2 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)

            # 重定向文件描述符
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # 注册退出函数，根据文件pid判断是否存在进程
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write('%s\n' % pid)

    def delpid(self):
        os.remove(self.pidfile)

    def start(self):
        # 检查pid文件是否存在以探测是否存在进程
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = 'pidfile %s already exist. Daemon already running!\n'
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)

            # 启动监控
        self._daemonize()
        self.procedure()

    def stop(self):
        # 从pid文件中获取pid
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:  # 重启不报错
            message = 'pidfile %s does not exist. Daemon not running!\n'
            sys.stderr.write(message % self.pidfile)
            return

            # 杀进程
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
                # os.system('hadoop-daemon.sh stop datanode')
                # os.system('hadoop-daemon.sh stop tasktracker')
                # os.remove(self.pidfile)
        except OSError, err:
            err = str(err)
            if err.find('No such process') > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)

    def restart(self):
        self.stop()
        self.start()

    @staticmethod
    def _run():
        """ run your fun"""
        while True:
            # fp=open('/tmp/result','a+')
            # fp.write('Hello World\n')
            sys.stdout.write('%s:hello world\n' % (time.ctime(),))
            sys.stdout.flush()
            time.sleep(2)


def test_run():
        """ run your fun"""
        while True:
            # fp=open('/tmp/result','a+')
            # fp.write('Hello World\n')
            sys.stdout.write('%s:hello world test run\n' % (time.ctime(),))
            sys.stdout.flush()
            time.sleep(2)

def run_as_daemon(op, pid_file, main):
    daemon = Daemon(pid_file, stdout='/tmp/watch_stdout.log')
    daemon.procedure = main
    if 'start' == op:
        daemon.start()
    elif 'stop' == op:
        daemon.stop()
    elif 'restart' == op:
        daemon.restart()
    else:
        print 'unknown command'
        sys.exit(2)
    sys.exit(0)

def run(name, method):
    if len(sys.argv) == 1:
        method()
    else:
        op = None
        if len(sys.argv) == 3:
            if sys.argv[1] == '-d' and ('start', 'stop', 'restart').count(sys.argv[2]) == 1:
                op = sys.argv[2]
        if op is not None:
            run_as_daemon(op, common.build_filename(name+".pid"), method)
        else:
            print 'usage: %s -d start|stop|restart' % sys.argv[0]
            sys.exit(2)
if __name__ == '__main__':
    run_as_daemon(sys.argv[1], '/tmp/watch_process.pid', test_run)