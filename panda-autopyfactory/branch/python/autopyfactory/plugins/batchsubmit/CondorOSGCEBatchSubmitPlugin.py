#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCEBatchSubmitPlugin import CondorCEBatchSubmitPlugin


class CondorOSGCEBatchSubmitPlugin(CondorCEBatchSubmitPlugin):
    id = 'condorosgce'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.factory.qcl            
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

        self.classads['GridResource']  = 'condor %s %s:9619' % (self.gridresource, self.gridresource))
        # in a line like  grid_resource condor neo.matrix.net neo.matrix.net:9619
        #   the first field is the schedd host
        #   the second field is the central manager host
        # we can assume for the time being they are the same. 

        # FIXME !!!
        #self.classads['TransferOutput'] = '""'
        self.classads['+TransferOutput'] = '""'
    
        super(CondorOSGCEBatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorOSGCEBatchSubmitPlugin.addJSD: Leaving.')

