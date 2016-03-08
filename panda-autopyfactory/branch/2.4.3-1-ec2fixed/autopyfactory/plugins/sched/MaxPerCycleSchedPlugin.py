#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class MaxPerCycleSchedPlugin(SchedInterface):
    id = 'maxpercycle'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)
            self.max_pilots_per_cycle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.maxpercycle.maximum', 'getint')
            self.log.trace("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        self.log.debug('Starting with n=%s' %n)

        orign = n
        out = n
        msg = None

        if self.max_pilots_per_cycle is not None:
            out = min(n, self.max_pilots_per_cycle)
            msg = "MaxPCycle:in=%s,max=%s,out=%s" %(orign, 
                                         self.max_pilots_per_cycle, 
                                         out)
                
        self.log.info(msg)
        return (out, msg)
