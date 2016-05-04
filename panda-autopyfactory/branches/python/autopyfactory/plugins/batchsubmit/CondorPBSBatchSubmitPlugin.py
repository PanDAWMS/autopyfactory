#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin


class CondorPBSBatchSubmitPlugin(CondorGridBatchSubmitPlugin):
    id = 'condorpbs'
    
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.factory.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorpbs', 'batchsubmit.condorgrid')
        super(CondorPBSBatchSubmitPlugin, self).__init__(apfqueue, config=newqcl)        

        self.log.info('CondorPBSBatchSubmitPlugin: Object initialized.')

    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorPBSBatchSubmitPlugin.addJSD: Starting.')

        self.classads['GridResource'] = 'pbs'
        super(CondorPBSBatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorPBSBatchSubmitPlugin.addJSD: Leaving.')


