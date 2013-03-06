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

class FixedSchedPlugin(SchedInterface):
    id = 'fixed'    
    
    def __init__(self, apfqueue):
        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)
            self.pilotspercycle = None

            if self.apfqueue.qcl.has_option(self.apfqueue.apfqname, 'sched.fixed.pilotspercycle'):
                self.pilotspercycle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.fixed.pilotspercycle', 'getint', logger=self.log)
                self.log.debug('SchedPlugin: there is a fixedPilotsPerCycle number setup to %s' %self.pilotspercycle)

            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, nsub=0):
        """ 
        returns always a fixed number of pilots
        """

        if self.pilotspercycle:
            out = self.pilotspercycle
        else:
            out = 0
            self.log.debug('calcSubmitNum: there is not a fixedPilotsPerCycle, returning 0')
        return out

