#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class MaxPerCycle(SchedInterface):
    id = 'maxpercycle'
    
    def __init__(self, apfqueue, config, section):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger('autopyfactory.sched.%s' %apfqueue.apfqname)
            self.max_pilots_per_cycle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.maxpercycle.maximum', 'getint')
            self.log.debug("SchedPlugin: Object initialized.")
        except Exception as ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        self.log.debug('Starting with n=%s' %n)

        orign = n
        out = n
        msg = msg = "MaxPerCycle:comment=Not set,in=%s" % n

        if self.max_pilots_per_cycle is not None:
            out = min(n, self.max_pilots_per_cycle)
            msg = "MaxPerCycle:in=%s,maxpercycle=%s,ret=%s" %(orign, 
                                         self.max_pilots_per_cycle, 
                                         out)
                
        self.log.info(msg)
        return (out, msg)
