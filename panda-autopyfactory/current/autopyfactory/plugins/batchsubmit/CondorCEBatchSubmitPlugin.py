#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin 
from autopyfactory import jsd 


class CondorCEBatchSubmitPlugin(CondorGridBatchSubmitPlugin):
   
    def __init__(self, apfqueue):

        super(CondorCEBatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorCEBatchSubmitPlugin: Object initialized.')
   
    def _readconfig(self, qcl):
        '''
        read the config loader object 
        '''

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorce', 'batchsubmit.condorgrid')
        valid = super(CondorCEBatchSubmitPlugin, self)._readconfig(newqcl)
        return valid

    def _addJSD(self):
        '''   
        add things to the JSD object
        '''   
 
        self.log.debug('CondorCEBatchSubmitPlugin.addJSD: Starting.')
   
        # -- fixed stuffs -- 
        self.JSD.add('+Nonessential = True')

        super(CondorCEBatchSubmitPlugin, self)._addJSD() 
    
        self.log.debug('CondorCEBatchSubmitPlugin.addJSD: Leaving.')
    
