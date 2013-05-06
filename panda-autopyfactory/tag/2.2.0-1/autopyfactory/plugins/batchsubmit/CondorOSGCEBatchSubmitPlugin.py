#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCEBatchSubmitPlugin import CondorCEBatchSubmitPlugin
from autopyfactory import jsd

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorOSGCEBatchSubmitPlugin(CondorCEBatchSubmitPlugin):
    id = 'condorosgce'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue):

        super(CondorOSGCEBatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorOSGCEBatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl=None):
        ''' 
        read the config loader object
        ''' 
        # Chosing the queue config object, depending on 
        if not qcl:
            qcl = self.apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorosgce', 'batchsubmit.condorce')
        valid = super(CondorOSGCEBatchSubmitPlugin, self)._readconfig(newqcl)
        if not valid:
            return False
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorosgce.gridresource', logger=self.log) 
            return True
        except:
            return False


    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorOSGCEBatchSubmitPlugin.addJSD: Starting.')

        self.JSD.add('grid_resource=condor %s' % self.gridresource) 
        #self.JSD.add('remote_universe = Local')
        self.JSD.add('+TransferOutput=""')
        super(CondorOSGCEBatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorOSGCEBatchSubmitPlugin.addJSD: Leaving.')

