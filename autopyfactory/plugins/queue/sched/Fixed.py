#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class Fixed(SchedInterface):
    id = 'fixed'    
    
    def __init__(self, apfqueue, config, section):
        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)
            self.pilotspercycle = None

            if self.apfqueue.qcl.has_option(self.apfqueue.apfqname, 'sched.fixed.pilotspercycle'):
                self.pilotspercycle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.fixed.pilotspercycle', 'getint')
                self.log.debug('SchedPlugin: there is a fixedPilotsPerCycle number setup to %s' %self.pilotspercycle)

            self.log.debug("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        """ 
        returns always a fixed number of pilots
        """

        if self.pilotspercycle:
            out = self.pilotspercycle
            msg = "Fixed:in=%s,ret=%s" %(n, out)
        else:
            out = 0
            msg = "Fixed:comment=nosetup,in=%s,ret=0" %(n)
            self.log.debug('there is not a fixedPilotsPerCycle, returning 0')

        return (out, msg)

