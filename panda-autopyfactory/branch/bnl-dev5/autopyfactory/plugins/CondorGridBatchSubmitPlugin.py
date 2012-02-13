#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorBatchSubmitPlugin import CondorBatchSubmitPlugin 
import jsd 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorGridBatchSubmitPlugin(CondorBaseBatchSubmitPlugin):
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, qcl):

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys(qcl, 'batchsubmit.condorgrid', 'batchsubmit.condorbase')
        super(CondorGridBatchSubmitPlugin, self).__init__(apfqueue, newqcl) 

        self.log.info('CondorGridBatchSubmitPlugin: Object initialized.')

    def _addJSD(self):
    
        self.log.debug('CondorGridBatchSubmitPlugin.addJSD: Starting.')
   
        super(CondorGridBatchSubmitPlugin, self)._addJSD()

        # -- fixed stuffs -- 
        self.JSD.add("universe=grid")
    
        self.log.debug('CondorGridBatchSubmitPlugin.addJSD: Leaving.')
    
