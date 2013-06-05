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

            self.min_pilots_pending = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.minpending.minimum', 'getint', logger=self.log)

            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):

        self.log.debug('calcSubmitNum: Starting with n=%s' %n)

        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        out = n
        msg = None

        if self.batchinfo is None:
            self.log.warning("self.batchinfo is None!")
        else:
            pending_pilots = self.batchinfo[self.apfqueue.apfqname].pending
            if self.min_pilots_pending:
                out = max(n, self.min_pilots_pending - pending_pilots)     
                msg = "MinPending=%s,min=%s,pend=%s,ret=%s" %(n, self.min_pilots_pending, pending_pilots, out)
        
        # Catch all to prevent negative numbers
        #if out < 0:
        #    self.log.info('calcSubmitNum: calculated output was negative. Returning 0')
        #    out = 0
            
        self.log.info('calcSubmitNum: (min_pilots_pending=%s; pending=%s) : Return = %s' %(self.min_pilots_pending, pending_pilots, out))
        return (out, msg) 
