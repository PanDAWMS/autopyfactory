#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class MinPerCycle(SchedInterface):
    id = 'minpercycle'
    
    def __init__(self, apfqueue, config, section):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)
            self.min_pilots_per_cycle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.minpercycle.minimum', 'getint')
            self.log.trace("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):

        self.log.trace('Starting with n=%s' %n)

        out = n
        msg = "MinPerCycle:comment=Not set"

        if self.min_pilots_per_cycle is not None:
            out = max(n, self.min_pilots_per_cycle)
            msg = "MinPerCycle:in=%s,minpercycle=%s,ret=%s" %(n, self.min_pilots_per_cycle, out)

               
        self.log.info(msg)
        return (out, msg)
