#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCEBatchSubmitPlugin import CondorCEBatchSubmitPlugin
from autopyfactory import jsd


class CondorOSGCEBatchSubmitPlugin(CondorCEBatchSubmitPlugin):
    id = 'condorosgce'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorosgce', 'batchsubmit.condorce')
        super(CondorOSGCEBatchSubmitPlugin, self).__init__(apfqueue, config=newqcl) 
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorosgce.gridresource') 
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        
        self.log.info('CondorOSGCEBatchSubmitPlugin: Object initialized.')

    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorOSGCEBatchSubmitPlugin.addJSD: Starting.')

        self.JSD.add('grid_resource', 'condor %s' % self.gridresource) 
        #self.JSD.add('remote_universe ', ' Local')
        self.JSD.add('+TransferOutput', '""')
        super(CondorOSGCEBatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorOSGCEBatchSubmitPlugin.addJSD: Leaving.')

