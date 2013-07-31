#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class MaxPendingSchedPlugin(SchedInterface):
    id = 'maxpending'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            self.max_pilots_pending = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.maxpending.maximum', 'getint')

            self.log.debug("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin: object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        self.log.debug('Starting with n=%s' %n)
        #batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)
        queueinfo = self.apfqueue.batchstatus_plugin.getInfo(queue = self.apfqueue.apfqname, maxtime = self.apfqueue.batchstatusmaxtime)
        out = n
        msg = None
    
        if not queueinfo:
            out = n
            msg = "MaxPending: No queueinfo."
        else:
            pending_pilots = queueinfo.pending
            
            if pending_pilots == 0:
                # if no pending, there may be free slots, so we impose no limit
                out = n
                msg = "MaxPending:in=%s,pend=0,ret=%s" %(n, out)
            else:
                if self.max_pilots_pending is not None:
                    out = min(n, self.max_pilots_pending - pending_pilots)     
                    msg = "MaxPending:in=%s,pend=%s,max=%s,ret=%s" %(n, pending_pilots, self.max_pilots_pending, out)
            
        self.log.info('Return=%s' %out)
        return (out, msg)
