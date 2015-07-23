#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGRAMBatchSubmitPlugin import CondorGRAMBatchSubmitPlugin


class CondorGT2BatchSubmitPlugin(CondorGRAMBatchSubmitPlugin):
    id = 'condorgt2'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.factory.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgt2', 'batchsubmit.condorgram').filterkeys('globusrsl.gram2', 'batchsubmit.condorgram.gram')
        super(CondorGT2BatchSubmitPlugin, self).__init__(apfqueue, config=newqcl) 
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorgt2.gridresource') 
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        self.log.info('CondorGT2BatchSubmitPlugin: Object initialized.')

    def _addJSD(self):
        '''
        add things to the JSD object
        '''
        self.log.debug('CondorGT2BatchSubmitPlugin.addJSD: Starting.')
        self.classads['GridResource'] = 'gt2 %s' % self.gridresource
        super(CondorGT2BatchSubmitPlugin, self)._addJSD()
        self.log.debug('CondorGT2BatchSubmitPlugin.addJSD: Leaving.')

