#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGRAM import CondorGRAM
from autopyfactory import jsd


class CondorGT5(CondorGRAM):
    id = 'condorgt5'
    """
    This class is expected to have separate instances for each PandaQueue object. 
    """
   
    def __init__(self, apfqueue, config, section):
        qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgt5', 'batchsubmit.condorgram').filterkeys('globusrsl.gram5', 'batchsubmit.condorgram.gram')
        super(CondorGT5, self).__init__(apfqueue, newqcl, section) 
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorgt5.gridresource') 
        except Exception as e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        self.log.info('CondorGT5: Object initialized.')

    def _addJSD(self):
        """
        add things to the JSD object
        """
        self.log.debug('CondorGT5.addJSD: Starting.')
        self.JSD.add('grid_resource', 'gt5 %s' % self.gridresource) 
        super(CondorGT5, self)._addJSD()
        self.log.debug('CondorGT5.addJSD: Leaving.')

