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

    def calcSubmitNum(self, nsub=0):

        self.log.debug('calcSubmitNum: Starting with nsub=%s' %nsub)

        if self.min_pilots_per_cycle:
            nsub = max(nsub, self.min_pilots_per_cycle)

        # Catch all to prevent negative numbers
        #if nsub < 0:
        #    self.log.info('calcSubmitNum: calculated output was negative. Returning 0')
        #    nsub = 0
                
        self.log.info('calcSubmitNum: return with nsub=%s' %nsub)
        return nsub 
