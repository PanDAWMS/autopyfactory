#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from autopyfactory import jsd


class CondorBosco(CondorLocal):
    id = 'condorbosco'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorbosco', 'batchsubmit.condorlocal')
        super(CondorBosco, self).__init__(apfqueue, config=newqcl) 
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorbosco.gridresource') 
            
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        
        self.log.info('CondorBosco: Object initialized.')

    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorBosco.addJSD: Starting.')

        self.JSD.add('grid_resource', 'condor %s %s:%s' % (self.gridresource, self.gridresource, self.port))
        # in a line like  grid_resource condor neo.matrix.net neo.matrix.net:9619
        #   the first field is the schedd host
        #   the second field is the central manager host
        # we can assume for the time being they are the same. 

        self.JSD.add('+TransferOutput', '""')
    
        super(CondorBosco, self)._addJSD()

        self.log.debug('CondorBosco.addJSD: Leaving.')

