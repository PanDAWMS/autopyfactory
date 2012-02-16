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

class CondorGT5BatchSubmitPlugin(CondorGRAMBatchSubmitPlugin):
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, qcl=None):

        self.id = 'condorgt5'        

        # Chosing the queue config object, depending on 
        # it was an input option or not.
        #       If it was passed as input option, then that is the config object. 
        #       If not, then it is extracted from the apfqueue object
        if not qcl:
            qcl = apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgt5', 'batchsubmit.condorgram')
        super(CondorGT5BatchSubmitPlugin, self).__init__(apfqueue, newqcl) 

        # Get from the config object the specific variables that apply to this class
        try:
            self.gridresource = qcl.get(self.apfqname, 'batchsubmit.condorgt5.gridresource') 
        except:
            self._valid = False

        self.log.info('CondorGT5BatchSubmitPlugin: Object initialized.')

    def _addJSD(self):
        '''
        add things to the JSD file
        '''

        self.log.debug('CondorGT5BatchSubmitPlugin.addJSD: Starting.')

        super(CondorGT5BatchSubmitPlugin, self)._addJSD()
        self.JSD.add('grid_resource=gt5 %s' % self.gridresource) 

        self.log.debug('CondorGT5BatchSubmitPlugin.addJSD: Leaving.')

