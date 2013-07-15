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

            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin: object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):

        self.log.debug('calcSubmitNum: Starting with n=%s' %n)

        batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        out = n
        msg = None
    
        pending_pilots = batchinfo[self.apfqueue.apfqname].pending
        if self.max_pilots_pending:
            out = min(n, self.max_pilots_pending - pending_pilots)     
            msg = "MaxPending=%s,pend=%s,max=%s,ret=%s" %(n, pending_pilots, self.max_pilots_pending, out)

        # Catch all to prevent negative numbers
        #if out < 0:
        #    self.log.info('calcSubmitNum: calculated output was negative. Returning 0')
        #    out = 0
            
        self.log.info('calcSubmitNum: Return=%s' %out)
        return (out, msg)
