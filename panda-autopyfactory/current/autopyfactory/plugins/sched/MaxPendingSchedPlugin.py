#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class MaxPendingSchedPlugin(SchedInterface):
    id = 'maxpending'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            self.max_pilots_pending = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.maxpending.maximum', 'getint', logger=self.log)

            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin: object initialization failed. Raising exception")
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
            if self.max_pilots_pending:
                out = min(n, self.max_pilots_pending - pending_pilots)     
                msg = "MaxPending=%s,pend=%s,max=%s,ret=%s" %(n, pending_pilots, self.max_pilots_pending, out)

        # Catch all to prevent negative numbers
        #if out < 0:
        #    self.log.info('calcSubmitNum: calculated output was negative. Returning 0')
        #    out = 0
            
        self.log.info('calcSubmitNum: return with out=%s' %out)
        return (out, msg)
