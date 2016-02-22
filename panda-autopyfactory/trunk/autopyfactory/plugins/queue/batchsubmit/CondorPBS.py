#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGrid import CondorGrid
from autopyfactory import jsd


class CondorPBS(CondorGrid):
    id = 'condorpbs'
    
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorpbs', 'batchsubmit.condorgrid')
        super(CondorPBS, self).__init__(apfqueue, config=newqcl)        

        self.log.info('CondorPBS: Object initialized.')

    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorPBS.addJSD: Starting.')

        self.JSD.add('grid_resource', 'pbs') 
        super(CondorPBS, self)._addJSD()

        self.log.debug('CondorPBS.addJSD: Leaving.')


