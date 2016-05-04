#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorBaseBatchSubmitPlugin import CondorBaseBatchSubmitPlugin 
from autopyfactory import jsd

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorLocalBatchSubmitPlugin(CondorBaseBatchSubmitPlugin):
    id = 'condorlocal'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
    
    def __init__(self, apfqueue):

        super(CondorLocalBatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorLocalBatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl=None):
        '''
        read the config loader object
        '''

        # Chosing the queue config object, depending on 
        if not qcl:
            qcl = self.apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorlocal', 'batchsubmit.condorbase')
        valid = super(CondorLocalBatchSubmitPlugin, self)._readconfig(newqcl) 
        return valid
        
    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorLocalBatchSubmitPlugin.addJSD: Starting.')

        self.JSD.add("universe=vanilla")
        self.JSD.add("should_transfer_files = IF_NEEDED")

        super(CondorLocalBatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorLocalBatchSubmitPlugin.addJSD: Leaving.')
    
