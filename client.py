#!/usr/bin/python
# -*- coding:utf-8 -*-
import re
import traceback
import socket
import select
import time
import common
import yaml
import os
import daemon


class ClientPoint:
    def __init__(self):
        self.logger = common.init_logger("client")
        self.logger.info("test")
        yaml_pathname = common.build_filename( "client.yaml")
        f = open(yaml_pathname)
        cfg = yaml.load(f)
        f.close()
        _addr, _port = cfg['proxy'].split(':')
        self.proxy_port = int(_port)
        self.proxy_addr = _addr
        self.my_name = cfg['user']
        self.registered = False
        self.url_parser = re.compile("((.*):)?(\d+)")

    def main(self):
        actions = {'pong': self.on_pong, "setup":self.on_setup, "registered":self.on_registered}
        while True:
            sock = common.connect_to(self.proxy_addr, self.proxy_port)
            if sock is None:
                time.sleep(10)
                continue
            self.logger.info("connected to %s:%d"%(self.proxy_addr, self.proxy_port))
            try:
                common.set_sock_buff_size(sock)
                #sock.setblocking(1)
                sock.settimeout(5)
                self.registered = False
                text = ''
                socks_to_read = [sock]
                send_ping = True
                while True:
                    if self.registered:
                        if send_ping:
                            sock.sendall("ping;")
                            send_ping = False
                        else:
                            send_ping = True
                    else:
                        sock.send("register %s;" % self.my_name)

                    readable, writable, exceptional = select.select(socks_to_read, [], [], 10)
                    if len(readable):
                        try:
                            data = common.recv(sock, common.BUFFER_SIZE)
                        except socket.error:
                            self.logger.error(traceback.format_exc())
                            break
                        if len(data) == 0:
                            self.logger.warn('read exception, disconnected by remote')
                            break
                        else:
                            text += data
                            while True:
                                action, params, text = common.get_action(text)
                                if action is None:
                                    break
                                if actions.has_key(action):
                                    actions[action](sock, params)
                            
                                    
            except:
                self.logger.error(traceback.format_exc())

            common.safe_close_socket(sock)

    def on_pong(self, sock, params):
        #print  'pong;'
        self.logger.info('pong')

    def on_setup(self, sck, params):
        waiting_id = ' '.join((params[0], params[1]))
        m = re.findall(self.url_parser, params[0])
        if len(m) == 0:
            # setup fail
            sck.send("built fail %s;"%waiting_id)
            self.logger.error("built fail %s;"%waiting_id)
            return
        addr = m[0][1]
        if len(addr) == 0:
            addr = "127.0.0.1"
        port = m[0][2]
        local_side_sck = common.connect_to(addr, port)
        if local_side_sck is None:
            # setup fail
            sck.send("built fail %s;"%waiting_id)
            self.logger.error("built fail %s;" % waiting_id)
            return
        # connect to proxy
        proxy_side_sck = common.connect_to(self.proxy_addr, self.proxy_port)
        if proxy_side_sck is None:
            # setup fail
            sck.send("built fail %s;"%waiting_id)
            self.logger.error("built fail %s;" % waiting_id)
            return

        sck.send("built ok %s;"%waiting_id)
        proxy_side_sck.send("tunnel %s %s;"%(self.my_name, waiting_id))
        self.logger.info("built ok %s;" % waiting_id)

        # set two scoket as forwarding pair
        forwarding = common.TcpForwarding(waiting_id)
        forwarding.sck_side_A = local_side_sck
        forwarding.sck_side_B = proxy_side_sck
        forwarding.start()

    def on_registered(self, sock, params):
        #print "tcp porting mapping %s"%" ".join(params)
        self.logger.info("tcp porting mapping %s"%" ".join(params))
        if params[0] == 'ok':
            self.registered = True


def main():
    c = ClientPoint()
    c.main()

if __name__ == "__main__":
    daemon.run('client', main)