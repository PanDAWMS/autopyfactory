#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGRAMBatchSubmitPlugin import CondorGRAMBatchSubmitPlugin
from autopyfactory import jsd


class CondorGT5BatchSubmitPlugin(CondorGRAMBatchSubmitPlugin):
    id = 'condorgt5'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgt5', 'batchsubmit.condorgram').filterkeys('globusrsl.gram5', 'batchsubmit.condorgram.gram')
        super(CondorGT5BatchSubmitPlugin, self).__init__(apfqueue, config=newqcl) 
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorgt5.gridresource') 
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        self.log.info('CondorGT5BatchSubmitPlugin: Object initialized.')

    def _addJSD(self):
        '''
        add things to the JSD object
        '''
        self.log.debug('CondorGT5BatchSubmitPlugin.addJSD: Starting.')
        self.JSD.add('grid_resource', 'gt5 %s' % self.gridresource) 
        super(CondorGT5BatchSubmitPlugin, self)._addJSD()
        self.log.debug('CondorGT5BatchSubmitPlugin.addJSD: Leaving.')

