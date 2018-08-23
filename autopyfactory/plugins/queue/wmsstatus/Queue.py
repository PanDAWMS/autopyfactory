#! /usr/bin/env python
# Added to support running module as script from arbitrary location. 
from os.path import dirname, realpath, sep, pardir
fullpathlist = realpath(__file__).split(sep)
prepath = sep.join(fullpathlist[:-4])
import sys
sys.path.insert(0, prepath)


import logging
import threading
import time
import traceback

from urllib import urlopen

from autopyfactory.interfaces import WMSStatusInterface
###from autopyfactory.info import WMSStatusInfo
###from autopyfactory.info import WMSQueueInfo
###from autopyfactory.info import SiteInfo
###from autopyfactory.info import CloudInfo
import autopyfactory.utils as utils



class Queue(threading.Thread, WMSStatusInterface):
    """
    """

    def __init__(self, apfqueue, config, section):

        try:
            self.apfqueue = apfqueue
            self.log = logging.getLogger('autopyfactory.wmsstatus.%s' %apfqueue.apfqname)
            self.log.debug("WMSStatusPlugin: Initializing object...")

            self.maxage = self.apfqueue.fcl.generic_get('Factory', 'wmsstatus.queue.maxage', default_value=360)
            self.sleeptime = self.apfqueue.fcl.getint('Factory', 'wmsstatus.queue.sleep')

            # number to be returned by this plugins
            # FIXME: pick a better name
            self.num = None

            self.wmsqname = self.apfqueue.qcl.get(apfqueue.apfqname, 'wmsstatus.queue.qname')
            self.wmsqueue = self.apfqueue.factory.apfqueuesmanager.queues[self.wmsqname]

            threading.Thread.__init__(self) # init the thread
            self.stopevent = threading.Event()

            self.log.info('WMSStatusPlugin: Object initialized.')
        except Exception, ex:
            self.log.error("WMSStatusPlugin object initialization failed. Raising exception")
            raise ex


    def getInfo(self, queue=None):
        """
        Returns current WMSStatusInfo object
    
        If the info recorded is older than that maxage,
        None is returned, 
        """

        self.log.debug('get: Starting')
        return self.num



    def run(self):
        """
        Main loop
        """
    
        self.log.debug('Starting.')
        while not self.stopevent.isSet():
            try:
                self._update()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
            time.sleep(self.sleeptime)
        self.log.debug('Leaving.')


    def _update(self):

        self.num = self.wmsqueue.wmsstatus()
