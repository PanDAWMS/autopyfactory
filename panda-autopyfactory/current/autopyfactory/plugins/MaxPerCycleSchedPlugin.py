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

class MaxPerCycleSchedPlugin(SchedInterface):
    id = 'maxpercycle'
    
    def __init__(self, apfqueue):

        self._valid = True
        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)
            self.max_pilots_per_cycle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.maxpercycle.maximum', 'getint', logger=self.log)
            self.log.info("SchedPlugin: Object initialized.")
        except:
            self._valid = False

    def calcSubmitNum(self, nsub=0):

        self.log.debug('calcSubmitNum: Starting with nsub=%s' %nsub)

        if self.max_pilots_per_cycle:
            nsub = min(nsub, self.max_pilots_per_cycle)

        # Catch all to prevent negative numbers
        #if nsub < 0:
        #    self.log.info('calcSubmitNum: calculated output was negative. Returning 0')
        #    nsub = 0
                
        self.log.info('calcSubmitNum: return with nsub=%s' %nsub)
        return nsub 
