#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGRAMBatchSubmitPlugin import CondorGRAMBatchSubmitPlugin
from autopyfactory import jsd

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorGT5BatchSubmitPlugin(CondorGRAMBatchSubmitPlugin):
    id = 'condorgt5'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue):

        super(CondorGT5BatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorGT5BatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl=None):
        ''' 
        read the config loader object
        ''' 
        # Chosing the queue config object, depending on 
        if not qcl:
            qcl = self.apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgt5', 'batchsubmit.condorgram').filterkeys('globusrsl.gram5', 'batchsubmit.condorgram.gram')
        valid = super(CondorGT5BatchSubmitPlugin, self)._readconfig(newqcl)
        if not valid:
            return False
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorgt5.gridresource', logger=self.log) 
            return True
        except:
            return False

    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorGT5BatchSubmitPlugin.addJSD: Starting.')

        self.JSD.add('grid_resource=gt5 %s' % self.gridresource) 
        super(CondorGT5BatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorGT5BatchSubmitPlugin.addJSD: Leaving.')

