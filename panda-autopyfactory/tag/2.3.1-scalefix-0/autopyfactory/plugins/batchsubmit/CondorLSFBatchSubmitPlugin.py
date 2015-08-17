#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin
from autopyfactory import jsd


class CondorLSFBatchSubmitPlugin(CondorGridBatchSubmitPlugin):
    id = 'condorlsf'
    
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorlsf', 'batchsubmit.condorgrid')
        super(CondorLSFBatchSubmitPlugin, self).__init__(apfqueue, config=newqcl)        

        self.log.info('CondorLSFBatchSubmitPlugin: Object initialized.')

    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorLSFBatchSubmitPlugin.addJSD: Starting.')

        self.JSD.add('grid_resource', 'lsf') 
        super(CondorLSFBatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorLSFBatchSubmitPlugin.addJSD: Leaving.')


