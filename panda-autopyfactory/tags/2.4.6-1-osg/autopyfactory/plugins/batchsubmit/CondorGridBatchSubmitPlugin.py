#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorBaseBatchSubmitPlugin import CondorBaseBatchSubmitPlugin 
from autopyfactory import jsd 


class CondorGridBatchSubmitPlugin(CondorBaseBatchSubmitPlugin):
   
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
       
               
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgrid', 'batchsubmit.condorbase')
        super(CondorGridBatchSubmitPlugin, self).__init__(apfqueue, newqcl) 
        self.log.info('CondorGridBatchSubmitPlugin: Object initialized.')

    def _addJSD(self):
        '''   
        add things to the JSD object
        '''   
 
        self.log.debug('CondorGridBatchSubmitPlugin.addJSD: Starting.')
   
        self.JSD.add("universe", "grid")
        super(CondorGridBatchSubmitPlugin, self)._addJSD()
    
        self.log.debug('CondorGridBatchSubmitPlugin.addJSD: Leaving.')
    
