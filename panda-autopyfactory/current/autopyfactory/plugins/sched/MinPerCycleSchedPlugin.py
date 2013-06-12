#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class MinPerCycleSchedPlugin(SchedInterface):
    id = 'minpercycle'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)
            self.min_pilots_per_cycle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.minpercycle.minimum', 'getint', logger=self.log)
            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):

        self.log.debug('calcSubmitNum: Starting with n=%s' %n)

        out = n
        msg = None

        if self.min_pilots_per_cycle:
            out = max(n, self.min_pilots_per_cycle)
            msg = "MinPerCycle=%s,min=%s,ret=%s" %(n, self.min_pilots_per_cycle, out)

               
        self.log.info('calcSubmitNum: return with out=%s' %out)
        return (out, msg)
