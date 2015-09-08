#!/usr/bin/env python 

from multiprocessing.connection import Listener
import time
import threading
import logging

class APFListener(threading.Thread):
    '''
    class to implement a loop
    waiting for client messages
    '''

    def __init__(self, factory):
        threading.Thread.__init__(self)
        self.stopevent = threading.Event()

        self.log = logging.getLogger('main.listener')

        self.factory = factory

        address = ('localhost', 6000)  # FIXME, most probably that should be a configuration variable
        self.listener = Listener(address, authkey='secret password')  # FIXME: do we really need authkey???
        self.listener._listener._socket.settimeout(5) # FIXME pick a good number. Should it be a configuration variable?

        self.log.debug('listener thread created')


    def run(self):
        '''
        main loop, triggered by call to method start()
        here we wait for client messages
        '''

        while not self.stopevent.isSet():
            time.sleep(5) # FIXME pick a good number
            try:
                conn = self.listener.accept()
                self.msg = conn.recv()
                self.log.info('message %s received by listener' %self.msg)
                if self.msg == 'reconfigure':
                    self.log.info('calling Factory method update()' )
                    self.factory.update()
                conn.close()
            except:
                # the _listener._socket timeout was reached
                # but we do not do anything, just keep looping
                pass      
