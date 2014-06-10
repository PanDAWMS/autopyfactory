#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin 
from autopyfactory import jsd 


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
        self.JSD.add('+Nonessential', 'True')
        self.JSD.add('grid_resource', 'condor %s %s:9619' % (self.gridresource, self.gridresource))
        # in a line like  grid_resource condor neo.matrix.net neo.matrix.net:9619
        #   the first field is the schedd host
        #   the second field is the central manager host
        # we can assume for the time being they are the same. 

        super(CondorCEBatchSubmitPlugin, self)._addJSD() 
    
        self.log.debug('CondorCEBatchSubmitPlugin.addJSD: Leaving.')
    
