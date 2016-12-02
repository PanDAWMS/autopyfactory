#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGrid import CondorGrid 
from autopyfactory import jsd 


class CondorCE(CondorGrid):
   
    def __init__(self, apfqueue, config, section):

        qcl = config
        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorce', 'batchsubmit.condorgrid')
        super(CondorCE, self).__init__(apfqueue, newqcl, section) 

        self.log.info('CondorCE: Object initialized.')
   

    def _addJSD(self):
        '''   
        add things to the JSD object
        '''   
 
        self.log.debug('CondorCE.addJSD: Starting.')
   
        # -- fixed stuffs -- 
        self.JSD.add('+Nonessential', 'True')

        super(CondorCE, self)._addJSD() 
    
        self.log.debug('CondorCE.addJSD: Leaving.')
    
