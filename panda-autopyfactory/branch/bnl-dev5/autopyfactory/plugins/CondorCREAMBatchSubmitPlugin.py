#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCEBatchSubmitPlugin import CondorCEBatchSubmitPlugin 
import autopyfactory.utils as utils
import jsd 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorCREAMBatchSubmitPlugin(CondorCEBatchSubmitPlugin):
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, qcl=None):

        self.id = 'condorcream'

        # Chosing the queue config object, depending on 
        # it was an input option or not.
        #       If it was passed as input option, then that is the config object. 
        #       If not, then it is extracted from the apfqueue object
        if not qcl:
            qcl = apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorcream', 'batchsubmit.condorce')
        super(CondorCREAMBatchSubmitPlugin, self).__init__(apfqueue, newqcl) 

        try:

            self.gridresource = qcl.get(self.apfqname, 'batchsubmit.condorcream.gridresource') 
            self.creamport = qcl.getint(self.apfqname, 'batchsubmit.condorcream.port')  
            self.creambatch = qcl.get(self.apfqname, 'batchsubmit.condorcream.batch')  
            self.queue = None
            if qcl.has_option(self.apfqname,'batchsubmit.condorcream.queue'):
                self.queue = qcl.get(self.apfqname, 'batchsubmit.condorcream.queue')
            
            self.log.info('CondorCREAMBatchSubmitPlugin: Object initialized.')
        except:
            self._valid = False
   

    def _addJSD(self):
    
        self.log.debug('CondorCREAMBatchSubmitPlugin.addJSD: Starting.')
    
        super(CondorCREAMBatchSubmitPlugin, self)._addJSD() 

        self.JSD.add('grid_resource=cream %s:%d/ce-cream/services/CREAM2 %s %s' % (self.gridresource, self.creamport, self.creambatch, self.queue)) 
    
        self.log.debug('CondorCREAMBatchSubmitPlugin.addJSD: Leaving.')

