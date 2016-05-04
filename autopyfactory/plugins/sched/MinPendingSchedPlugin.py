#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging

class MinPendingSchedPlugin(SchedInterface):
    id = 'minpending'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            self.min_pilots_pending = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.minpending.minimum', 'getint')

            self.log.trace("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):

        self.log.trace('Starting with n=%s' %n)
        self.queueinfo = self.apfqueue.batchstatus_plugin.getInfo(queue = self.apfqueue.apfqname, 
                                                                  maxtime = self.apfqueue.batchstatusmaxtime)
        out = n
        msg = None
        if self.queueinfo is not None:
            pending_pilots = self.queueinfo.pending
            if self.min_pilots_pending is not None:
                out = max(n, self.min_pilots_pending - pending_pilots)     
                msg = "MinPending:in=%s,min=%s,pend=%s,out=%s" %(n, 
                                                                  self.min_pilots_pending, 
                                                                  pending_pilots, 
                                                                  out)
            else:
                msg = "MinPending:minpending not set."
        else:
            msg = "MinPending:Queueinfo is None."
        
        self.log.info(msg)
        return (out, msg) 
