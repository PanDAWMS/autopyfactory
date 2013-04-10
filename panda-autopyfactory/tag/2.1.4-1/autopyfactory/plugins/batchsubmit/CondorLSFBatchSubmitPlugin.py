#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin
from autopyfactory import jsd

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorLSFBatchSubmitPlugin(CondorGridBatchSubmitPlugin):
    id = 'condorlsf'
    
    def __init__(self, apfqueue):

        super(CondorLSFBatchSubmitPlugin, self).__init__(apfqueue)
        self.log.info('CondorLSFBatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl=None):
        '''
        read the config file
        '''

        # Chosing the queue config object, depending on 
        if not qcl:
            qcl = self.apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorlsf', 'batchsubmit.condorgrid')
        valid = super(CondorLSFBatchSubmitPlugin, self)._readconfig(qcl)
        return valid


    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorLSFBatchSubmitPlugin.addJSD: Starting.')

        self.JSD.add('grid_resource=lsf') 
        super(CondorLSFBatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorLSFBatchSubmitPlugin.addJSD: Leaving.')


