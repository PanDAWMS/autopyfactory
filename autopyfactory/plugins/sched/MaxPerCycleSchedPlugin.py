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

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)
            self.max_pilots_per_cycle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.maxpercycle.maximum', 'getint', logger=self.log)
            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, nsub=0):
        self.log.debug('calcSubmitNum: Starting with nsub=%s' %nsub)
        input = nsub

        if self.max_pilots_per_cycle:
            nsub = min(nsub, self.max_pilots_per_cycle)
                
        self.log.info('calcSubmitNum: input=%s ;return=%s' % (input, nsub))
        return nsub 
