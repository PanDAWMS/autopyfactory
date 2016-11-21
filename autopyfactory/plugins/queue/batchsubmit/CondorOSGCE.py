#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCE import CondorCE
from autopyfactory import jsd


class CondorOSGCE(CondorCE):
    id = 'condorosgce'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, config, section):
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorosgce', 'batchsubmit.condorce')
        super(CondorOSGCE, self).__init__(apfqueue, config=newqcl, section) 
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorosgce.gridresource') 
            self.port = qcl.generic_get(self.apfqname, 'batchsubmit.condorosgce.port', default_value='9619') 
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        
        self.log.info('CondorOSGCE: Object initialized.')

    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorOSGCE.addJSD: Starting.')

        self.JSD.add('grid_resource', 'condor %s %s:%s' % (self.gridresource, self.gridresource, self.port))
        # in a line like  grid_resource condor neo.matrix.net neo.matrix.net:9619
        #   the first field is the schedd host
        #   the second field is the central manager host
        # we can assume for the time being they are the same. 

        self.JSD.add('+TransferOutput', '""')
    
        super(CondorOSGCE, self)._addJSD()

        self.log.debug('CondorOSGCE.addJSD: Leaving.')

