'''Intercepts ZeroMQ traffic'''

import logging
import inspect
import os
import time

import tornado.gen
import zmq

import salt.payload
from salt.channel.client import AsyncPubChannel
from salt.channel.client import AsyncReqChannel

log = logging.getLogger(__name__)

class Vampire(object):
    '''Intercepts traffic to and from the minion via monkey patching and sends it into the Proxy.'''

    def __init__(self):
        self.context = zmq.Context.instance()

    def attach(self):
        '''Monkey-patches ZeroMQ core I/O class to capture flowing messages.'''
        AsyncReqChannel.dump = self.dump
        AsyncReqChannel._original_send = AsyncReqChannel.send
        AsyncReqChannel.send = _dumping_send
        AsyncReqChannel._original_crypted_transfer_decode_dictentry = AsyncReqChannel.crypted_transfer_decode_dictentry
        AsyncReqChannel.crypted_transfer_decode_dictentry = _dumping_crypted_transfer_decode_dictentry

        AsyncPubChannel.dump = self.dump
        AsyncPubChannel._original_on_recv = AsyncPubChannel.on_recv
        AsyncPubChannel.on_recv = _dumping_on_recv

    def dump(self, load, socket, method, **kwargs):
        '''Dumps a ZeroMQ message to the Proxy'''

        header = {
            'socket' : socket,
            'time' : time.time(),
            'pid' : os.getpid(),
            'method': method,
            'kwargs': kwargs,
        }
        event = {
            'header' : header,
            'load' : load,
        }

        try:
            zsocket = self.context.socket(zmq.PUSH)
            zsocket.connect('ipc:///tmp/evil-minions-pull.ipc')
            zsocket.send(salt.payload.dumps(event), flags=zmq.NOBLOCK)
            zsocket.close(linger=0)
        except Exception as exc:
            log.error("Event: {}".format(event))
            log.error("Unable to dump event: {}".format(exc))

@tornado.gen.coroutine
def _dumping_send(self, load, **kwargs):
    '''Dumps a REQ ZeroMQ and sends it'''
    self.dump(load, 'REQ', 'send', **kwargs)
    ret = yield self._original_send(load, **kwargs)
    raise tornado.gen.Return(ret)

@tornado.gen.coroutine
def _dumping_crypted_transfer_decode_dictentry(self, load, **kwargs):
    '''Dumps a REQ crypted ZeroMQ message and sends it'''
    self.dump(load, 'REQ', 'crypted_transfer_decode_dictentry', **kwargs)
    ret = yield self._original_crypted_transfer_decode_dictentry(load, **kwargs)
    raise tornado.gen.Return(ret)

def _dumping_on_recv(self, callback):
    '''Dumps a PUB ZeroMQ message then handles it'''
    async def _logging_callback(load):
        self.dump(load, 'PUB', 'on_recv')
        result = callback(load)
        if inspect.isawaitable(result):
            await result
    return self._original_on_recv(_logging_callback)
