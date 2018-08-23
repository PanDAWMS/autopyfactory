#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGrid import CondorGrid
from autopyfactory import jsd


class CondorLSF(CondorGrid):
    id = 'condorlsf'
    
    def __init__(self, apfqueue, config, section):

        qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorlsf', 'batchsubmit.condorgrid')
        super(CondorLSF, self).__init__(apfqueue, newqcl, section)
        self.log.info('CondorLSF: Object initialized.')

    def _addJSD(self):
        """
        add things to the JSD object
        """
        self.log.debug('CondorLSF.addJSD: Starting.')
        self.JSD.add('grid_resource', 'lsf') 
        super(CondorLSF, self)._addJSD()
        self.log.debug('CondorLSF.addJSD: Leaving.')


