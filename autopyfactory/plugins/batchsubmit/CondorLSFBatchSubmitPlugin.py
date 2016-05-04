#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin


class CondorLSFBatchSubmitPlugin(CondorGridBatchSubmitPlugin):
    id = 'condorlsf'
    
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.factory.qcl            
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

        self.classads['GridResource'] = 'lsf'
        super(CondorLSFBatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorLSFBatchSubmitPlugin.addJSD: Leaving.')


