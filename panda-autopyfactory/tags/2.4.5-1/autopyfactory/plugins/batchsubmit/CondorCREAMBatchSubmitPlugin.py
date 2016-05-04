#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCEBatchSubmitPlugin import CondorCEBatchSubmitPlugin 
import autopyfactory.utils as utils
from autopyfactory import jsd 


class CondorCREAMBatchSubmitPlugin(CondorCEBatchSubmitPlugin):
    id = 'condorcream'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorcream', 'batchsubmit.condorce')
        super(CondorCREAMBatchSubmitPlugin, self).__init__(apfqueue, config=newqcl) 
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorcream.gridresource') 
            self.webservice = qcl.generic_get(self.apfqname, 'batchsubmit.condorcream.webservice')
            self.creamport = qcl.generic_get(self.apfqname, 'batchsubmit.condorcream.port', 'getint')
            self.creambatch = qcl.generic_get(self.apfqname, 'batchsubmit.condorcream.batch')
            self.queue = qcl.generic_get(self.apfqname, 'batchsubmit.condorcream.queue')
        
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        
        self.log.info('CondorCREAMBatchSubmitPlugin: Object initialized.')

          
    def _addJSD(self):
        '''
        add things to the JSD object
        '''
        self.log.debug('CondorCREAMBatchSubmitPlugin.addJSD: Starting.')
        # if variable webservice, for example, has a value, 
        # then we can assume the grid resource line is meant to be built from pieces.
        # Otherwise, we will assume its entire value comes from gridresource variable. 
        if self.webservice:
                self.JSD.add('grid_resource', 'cream %s:%d/ce-cream/services/CREAM2 %s %s' % (self.webservice, self.creamport, self.creambatch, self.queue))
        else:
                self.JSD.add('grid_resource', 'cream %s' %self.gridresource)
        super(CondorCREAMBatchSubmitPlugin, self)._addJSD() 
        self.log.debug('CondorCREAMBatchSubmitPlugin.addJSD: Leaving.')

