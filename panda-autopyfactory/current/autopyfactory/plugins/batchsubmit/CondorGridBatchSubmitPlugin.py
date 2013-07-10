#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorBaseBatchSubmitPlugin import CondorBaseBatchSubmitPlugin 
from autopyfactory import jsd 


class CondorGridBatchSubmitPlugin(CondorBaseBatchSubmitPlugin):
   
    def __init__(self, apfqueue):

        super(CondorGridBatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorGridBatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl):
        '''
        read the config loader object
        '''

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgrid', 'batchsubmit.condorbase')
        valid = super(CondorGridBatchSubmitPlugin, self)._readconfig(newqcl) 
        return valid

    def _addJSD(self):
        '''   
        add things to the JSD object
        '''   
 
        self.log.debug('CondorGridBatchSubmitPlugin.addJSD: Starting.')
   
        self.JSD.add("universe", "grid")
        super(CondorGridBatchSubmitPlugin, self)._addJSD()
    
        self.log.debug('CondorGridBatchSubmitPlugin.addJSD: Leaving.')
    
