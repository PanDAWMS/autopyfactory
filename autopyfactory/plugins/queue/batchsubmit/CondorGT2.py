#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGRAM import CondorGRAM
from autopyfactory import jsd


class CondorGT2(CondorGRAM):
    id = 'condorgt2'
    """
    This class is expected to have separate instances for each PandaQueue object. 
    """
   
    def __init__(self, apfqueue, config, section):
        qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgt2', 'batchsubmit.condorgram').filterkeys('globusrsl.gram2', 'batchsubmit.condorgram.gram')
        super(CondorGT2, self).__init__(apfqueue, newqcl, section) 
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorgt2.gridresource') 
        except Exception as e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        self.log.info('CondorGT2: Object initialized.')

    def _addJSD(self):
        """
        add things to the JSD object
        """
        self.log.debug('CondorGT2.addJSD: Starting.')
        self.JSD.add('grid_resource', 'gt2 %s' % self.gridresource) 
        super(CondorGT2, self)._addJSD()
        self.log.debug('CondorGT2.addJSD: Leaving.')

