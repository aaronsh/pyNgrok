#!/usr/bin/python
# -*- coding:utf-8 -*-
import Queue
import SocketServer
import json
import re
import socket
import threading
import traceback
import select
import os

import time
import yaml
import atexit
import logging
import logging.config

import cust_logging
import common
import daemon


class HostUnderNAT(threading.Thread):
    def __init__(self, logger, name):
        super(HostUnderNAT, self).__init__(name="HostUnderNAT")
        self.name = name
        self.logger = logger
        self.listening_scks = {}
        self.building_tunnels = {}
        self.base_sck = None
        self.data_from_host = ''
        self.running = threading.Event()
        self.lock = threading.Lock()
        self.http_request_queue = Queue.Queue()

    def terminate(self):
        self.running.clear()

    def open_ports_for_tcp_forwarding(self, ports):
        mapping = []
        for pair in ports:
            try:
                sck = common.listen_on(pair[0])
                if sck is not None:
                    redirection = (pair[0], str(pair[1]))
                    self.listening_scks[sck] = redirection
                    mapping.append(redirection)
            except:
                self.logger.error(traceback.format_exc())
        return mapping

    def add_http_tunnel_req(self, sock, data, port):
        self.http_request_queue.put((sock, data, port))

    def __get_waiting_sock(self, waiting_id):
        pair = None
        self.lock.acquire()
        try:
            pair = self.building_tunnels[waiting_id]
            del self.building_tunnels[waiting_id]
        except:
            self.logger.error(traceback.format_exc())
        self.lock.release()
        return pair

    def __add_waiting_sock(self, waiting_id, sock, data=None):
        old_sock = None
        old_data = None
        self.lock.acquire()
        if self.building_tunnels.has_key(waiting_id):
            old_sock, old_data = self.building_tunnels[waiting_id]
        self.building_tunnels[waiting_id] = (sock, data)
        self.lock.release()
        if old_sock is not None:
            common.safe_close_socket(old_sock)
        if old_data is not None:
            del old_data

    def __del_waiting_sock(self, waiting_id):
        old_sock = None
        old_data = None
        self.lock.acquire()
        try:
            if self.building_tunnels.has_key(waiting_id):
                old_sock, old_data = self.building_tunnels[waiting_id]
                del self.building_tunnels[waiting_id]
        except:
            self.logger.error(traceback.format_exc())
            pass
        self.lock.release()
        if old_sock is not None:
            common.safe_close_socket(old_sock)
        if old_data is not None:
            del old_data

    def on_tunnel_established(self, sock, waiting_id, cached_tcp_data):
        pair = self.__get_waiting_sock(waiting_id)
        if pair is not None:
            forwarding = common.TcpForwarding(waiting_id)
            forwarding.sck_side_A = sock
            if len(cached_tcp_data):
                forwarding.cached_data_from_A = cached_tcp_data
            forwarding.sck_side_B = pair[0]
            if pair[1] is not None:
                forwarding.cached_data_from_B = pair[1]
            forwarding.start()
        else:
            common.safe_close_socket(sock)

    def run(self):
        actions = {"ping": self.on_ping, 'built': self.on_built}
        self.running.set()
        self.logger.info("thread of host %s begin"%self.name)
        while self.running.isSet():
            try:
                inputs = self.listening_scks.keys()
                inputs.append(self.base_sck)
                excepts = inputs[:]
                readable,writable, exceptional = select.select(inputs,[], excepts, 1)
                for s in exceptional:
                    self.running.clear()
                    break
                for s in readable:
                    if s == self.base_sck:
                        self.__proc_host_actions(actions)
                    else:
                        #new connect
                        client,addr=s.accept()
                        common.set_sock_buff_size(client)
                        outer_port, redirect_to = self.listening_scks[s]
                        waiting_id = "%s %s"%(redirect_to, common.key_gen())
                        self.__add_waiting_sock(waiting_id, client)
                        self.base_sck.send("setup %s;"%waiting_id)
                        self.logger.info("setup tcp %s;"%waiting_id)
                #setup http tunnel
                while True:
                    try:
                        http_sock, data, redirect_to = self.http_request_queue.get(block=False)
                        waiting_id = "%s %s" % (redirect_to, common.key_gen())
                        self.__add_waiting_sock(waiting_id, http_sock, data)
                        self.base_sck.send("setup %s;"%waiting_id)
                        self.logger.info("setup http %s;"%waiting_id)
                    except Queue.Empty, e:
                        #traceback.print_exc()
                        break
            except socket.error,e:
                self.logger.error(traceback.format_exc())
                self.running.clear()
        #clear resources
        socks = self.listening_scks.keys()
        for s in socks:
            common.safe_close_socket(s)
        common.safe_close_socket(self.base_sck)
        self.logger.info("thread of host %s end"%self.name)

    def __proc_host_actions(self, actions):
        try:
            data = common.recv(self.base_sck, common.BUFFER_SIZE)
            if len(data):
                text = self.data_from_host + data
                while True:
                    action, params, text = common.get_action(text)
                    if action is None:
                        break
                    if actions.has_key(action):
                        actions[action](params)
                self.data_from_host = text
            else:
                # 与host的连接断开
                self.logger.info(u'与host的连接断开')
                self.running.clear()
        except socket.error, e:
            self.running.clear()
            self.logger.error(traceback.format_exc())

    def on_ping(self, params):
        self.base_sck.sendall("pong;")
        pass

    def on_built(self, params):
        if params[0] == "fail":
            waiting_id = " ".join((params[1], params[2]))
            pair = self.__get_waiting_sock(waiting_id)
            if pair is not None:
                sock = pair[0]
                common.safe_close_socket(sock)


class Proxy(threading.Thread):
    def __init__(self, logger):
        super(Proxy, self).__init__(name="HostUnderNAT")
        self.logger = logger

        yaml_pathname = common.build_filename("server.yaml")
        f = open(yaml_pathname)
        self.cfg = yaml.load(f)
        f.close()

        self.base_sck = common.listen_on(self.cfg['proxy_port'])
        self.http_sck = common.listen_on(self.cfg['http_port'])
        self.cmd_sck = common.listen_on(self.cfg['command_port'], '127.0.0.1')
        self.new_socks = []
        self.data_of_new_socks = {}
        self.sock_reader = {str(self.base_sck):self.on_accept_new_host,
                            str(self.http_sck):self.on_accept_new_http_req,
                            str(self.cmd_sck):self.on_accept_new_command}
        self.actions = {'register': self.on_register, 'tunnel': self.on_tunnel, 'ping':self.on_ping,
                            'help': self.on_help, 'h': self.on_help, 'list': self.on_list, 'stop':self.on_stop}
        self.hosts = {}
        self.domain_to_host = {}
        self.running = threading.Event()
        self.re_host_patten = re.compile("^Host: (\\S*)\\s*$", re.MULTILINE)
        self.__load_domain_mapping()

    def terminate(self):
        self.running.clear()

    def run(self):
        self.running.set()
        if self.base_sck is not None and self.http_sck is not None and self.cmd_sck is not None:
            while self.running.isSet():
                socks = [self.base_sck, self.http_sck, self.cmd_sck]
                socks.extend(self.new_socks)
                try:
                    readable, writable, exceptional = select.select(socks, [], socks, 1)
                    for s in readable:
                        sock_str = str(s)
                        if self.sock_reader.has_key(sock_str):
                            self.sock_reader[sock_str](s)
                except socket.error:
                    #traceback.print_exc()
                    self.logger.error(traceback.format_exc())
        common.safe_close_socket(self.http_sck)
        common.safe_close_socket(self.base_sck)
        common.safe_close_socket(self.cmd_sck)
        for s in self.new_socks:
            common.safe_close_socket(s)
        for h in self.hosts.values():
            h.terminate()
            h.join()

    def proc_host_connection(self, sck):
        sck_id = str(sck)
        data = common.recv(sck, common.BUFFER_SIZE)
        if len(data):
            text = self.data_of_new_socks[sck_id] + data
            while True:
                action, params, text = common.get_action(text)
                self.data_of_new_socks[sck_id] = text
                if action is None:
                    break
                if self.actions.has_key(action):
                    self.actions[action](sck, params, text)
        else:
            self.__remove_new_sock(sck)
            common.safe_close_socket(sck)

    def on_accept_new_host(self, sock):
        # incoming new connection from host
        client, addr = self.base_sck.accept()
        common.set_sock_buff_size(client)
        self.logger.info(self.name+" accept connection from host "+str(addr))
        self.__save_new_sock(client, self.proc_host_connection )

    def proc_cmd_connection(self, sck):
        sck_id = str(sck)
        data = common.recv(sck, common.BUFFER_SIZE)
        if len(data):
            text = self.data_of_new_socks[sck_id] + data
            while True:
                action, params, text = common.get_action(text, "\r\n")
                self.data_of_new_socks[sck_id] = text
                if action is None:
                    break
                if self.actions.has_key(action):
                    self.actions[action](sck, params, text)
        else:
            self.__remove_new_sock(sck)
            common.safe_close_socket(sck)

    def on_accept_new_command(self, sock):
        # incoming new connection from host
        client, addr = self.cmd_sck.accept()
        common.set_sock_buff_size(client)
        self.__save_new_sock(client, self.proc_cmd_connection )

    def on_accept_new_http_req(self, sock):
        # incoming new connection from host
        client, addr = self.http_sck.accept()
        common.set_sock_buff_size(client)
        self.__save_new_sock(client, self.proc_http_connection)

    def proc_http_connection(self, sock):
        sck_id = str(sock)
        data = common.recv(sock, common.BUFFER_SIZE)
        if len(data):
            self.data_of_new_socks[sck_id] = self.data_of_new_socks[sck_id] + data
            if self.data_of_new_socks[sck_id].find("\r\n\r\n") > 0:
                self.logger.info("received http header\n"+self.data_of_new_socks[sck_id].split("\r\n\r\n")[0])
                host = re.findall(self.re_host_patten, self.data_of_new_socks[sck_id])
                if len(host):
                    domain = host[0]
                    if self.domain_to_host.has_key(domain):
                        host_name, port = self.domain_to_host[domain]
                        if self.hosts.has_key(host_name):
                            self.hosts[host_name].add_http_tunnel_req(sock, self.data_of_new_socks[sck_id], port)
                        self.__remove_new_sock(sock)
                    else:
                        self.__remove_new_sock(sock)
                        common.safe_close_socket(sock)
        else:
            self.__remove_new_sock(sock)
            common.safe_close_socket(sock)

    def on_register(self, sock, params, extra_data):
        host_name = params[0]
        if self.hosts.has_key(host_name):
            self.hosts[host_name].terminate()
            self.hosts[host_name].join()
            del self.hosts[host_name]
        self.logger.info("receive register request for %s"%host_name)
        #check host
        if self.cfg['users'].has_key(host_name):
            #create host object
            host = HostUnderNAT(self.logger, host_name)
            mapping = host.open_ports_for_tcp_forwarding(self.cfg['users'][host_name]['tcp'])
            if len(mapping) == 0:
                sock.send("registered fail;")
                self.logger.info("registered %s fail;"%host_name)
                self.__remove_new_sock(sock)
                common.safe_close_socket(sock)
            else:
                host.base_sck = sock
                maps = []
                for item in mapping:
                    maps.append('%s->%s'%item)
                sock.send("registered ok %s;"%" ".join(maps))
                self.logger.info("registered %s ok;" % host_name)
                self.__remove_new_sock(sock)
                host.start()
                self.hosts[host_name] = host
        else:
            sock.send("registered fail;")
            self.logger.info("registered %s fail;" % host_name)
            self.__remove_new_sock(sock)
            common.safe_close_socket(sock)

    def on_tunnel(self, sock, params, extra_data):
        self.logger.info("on tunnel %s" % ' '.join(params))
        host_name = params[0]
        if self.hosts.has_key(host_name):
            waiting_id = ' '.join((params[1], params[2]))
            self.hosts[host_name].on_tunnel_established(sock, waiting_id, extra_data)
            self.__remove_new_sock(sock)
        else:
            self.__remove_new_sock(sock)
            common.safe_close_socket(sock)

    def on_ping(self, sock, params, extra_data):
        sock.sendall("force closed!")
        self.__remove_new_sock(sock)
        common.safe_close_socket(sock)

    def on_stop(self, sock, params, extra_data):
        sock.sendall("terminating the server now!\r\n")
        self.running.clear()

    def on_help(self, sock, params, extra_data):
        help = '''
you can type such commands like "help, h, list, stop"
    stop
        stop the server immediately
    help
        show details of help
    list
        list clients, -list the clients which is connecting with the server
'''
        sock.sendall(help)

    def on_list(self, sock, params, extra_data):
        help = '''
not implymented yet!
'''
        sock.sendall(help)

    def __save_new_sock(self, sock, proc):
        sock_str = str(sock)
        self.data_of_new_socks[sock_str] = ''
        self.sock_reader[sock_str] = proc
        self.new_socks.append(sock)

    def __remove_new_sock(self, sock):
        sck_id = str(sock)
        del self.sock_reader[sck_id]
        del self.data_of_new_socks[sck_id]
        self.new_socks.remove(sock)

    def __load_domain_mapping(self):
        for usr in self.cfg['users']:
            for m in self.cfg['users'][usr]['http']:
                self.domain_to_host[m[0]] = (usr, m[1])


def main_shutdown():
    print "main shutdown"


def main():
    # 注册退出回调函数
    #atexit.register(main_shutdown)
    logger = common.init_logger("server")
    proxy = Proxy(logger)
    proxy.start()
    proxy.join()
    logger.info('server end')

if __name__ == "__main__":
    daemon.run('server', main)
