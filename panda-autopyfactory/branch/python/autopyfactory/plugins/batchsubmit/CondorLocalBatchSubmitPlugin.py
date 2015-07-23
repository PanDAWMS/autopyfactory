#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorBaseBatchSubmitPlugin import CondorBaseBatchSubmitPlugin 
from autopyfactory import jsd


class CondorLocalBatchSubmitPlugin(CondorBaseBatchSubmitPlugin):
    id = 'condorlocal'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
    
    def __init__(self, apfqueue, config=None):
        
        if not config:
            qcl = apfqueue.factory.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorlocal', 'batchsubmit.condorbase')

        super(CondorLocalBatchSubmitPlugin, self).__init__(apfqueue, config=newqcl) 
        self.log.info('CondorLocalBatchSubmitPlugin: Object initialized.')

       
    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorLocalBatchSubmitPlugin.addJSD: Starting.')

        self.classads['JobUniverse'] = 5
        self.classads['should_transfer_files'] = "IF_NEEDED"
        self.classads['+TransferOutput'] = '""'

        super(CondorLocalBatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorLocalBatchSubmitPlugin.addJSD: Leaving.')
    
