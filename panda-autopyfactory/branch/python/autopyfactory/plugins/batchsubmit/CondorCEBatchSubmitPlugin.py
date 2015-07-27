#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin 


class CondorCEBatchSubmitPlugin(CondorGridBatchSubmitPlugin):
   
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.factory.qcl            
        else:
            qcl = config
            
        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorce', 'batchsubmit.condorgrid')
        super(CondorCEBatchSubmitPlugin, self).__init__(apfqueue, config=newqcl) 

        self.log.info('CondorCEBatchSubmitPlugin: Object initialized.')
   

    def _addJSD(self):
        '''   
        add things to the JSD object
        '''   
 
        self.log.debug('CondorCEBatchSubmitPlugin.addJSD: Starting.')
   
        # -- fixed stuffs -- 
        self.classads['Nonessential'] = 'true'

        super(CondorCEBatchSubmitPlugin, self)._addJSD() 
    
        self.log.debug('CondorCEBatchSubmitPlugin.addJSD: Leaving.')
    
