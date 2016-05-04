#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGRAMBatchSubmitPlugin import CondorGRAMBatchSubmitPlugin
import jsd 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorGT2BatchSubmitPlugin(CondorGRAMBatchSubmitPlugin):
    id = 'condorgt2'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue):

        super(CondorGT2BatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorGT2BatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl=None):
        ''' 
        read the config loader object
        ''' 
        # Chosing the queue config object, depending on 
        if not qcl:
            qcl = self.apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgt2', 'batchsubmit.condorgram')
        valid = super(CondorGT2BatchSubmitPlugin, self)._readconfig(newqcl)
        if not valid:
                return False
        try:
                self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorgt2.gridresource', logger=self.log) 
                return True
        except:
                return False


    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorGT2BatchSubmitPlugin.addJSD: Starting.')

        super(CondorGT2BatchSubmitPlugin, self)._addJSD()
        self.JSD.add('grid_resource=gt2 %s' % self.gridresource) 

        self.log.debug('CondorGT2BatchSubmitPlugin.addJSD: Leaving.')

