#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCEBatchSubmitPlugin import CondorCEBatchSubmitPlugin 
import autopyfactory.utils as utils
from autopyfactory import jsd 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorCREAMBatchSubmitPlugin(CondorCEBatchSubmitPlugin):
    id = 'condorcream'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue):

        super(CondorCREAMBatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorCREAMBatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl=None):
        ''' 
        read the config loader object
        ''' 

        # Chosing the queue config object, depending on 
        if not qcl:
            qcl = self.apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorcream', 'batchsubmit.condorce')
        valid = super(CondorCREAMBatchSubmitPlugin, self)._readconfig(newqcl) 
        if not valid:
            return False
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorcream.gridresource', logger=self.log) 
            self.webservice = qcl.generic_get(self.apfqname, 'batchsubmit.condorcream.webservice', logger=self.log)
            self.creamport = qcl.generic_get(self.apfqname, 'batchsubmit.condorcream.port', 'getint', logger=self.log)
            self.creambatch = qcl.generic_get(self.apfqname, 'batchsubmit.condorcream.batch', logger=self.log)
            self.queue = qcl.generic_get(self.apfqname, 'batchsubmit.condorcream.queue', logger=self.log)

            return True
        except:
            return False
            

    def _addJSD(self):
        '''
        add things to the JSD object
        '''
    
        self.log.debug('CondorCREAMBatchSubmitPlugin.addJSD: Starting.')
   
        # if variable webservice, for example, has a value, 
        # then we can assume the grid resource line is meant to be built from pieces.
        # Otherwise, we will assume its entire value comes from gridresource variable. 
        if self.webservice:
                self.JSD.add('grid_resource=cream %s:%d/ce-cream/services/CREAM2 %s %s' % (self.webservice, self.creamport, self.creambatch, self.queue))
        else:
                self.JSD.add('grid_resource=cream %s' %self.gridresource)
        super(CondorCREAMBatchSubmitPlugin, self)._addJSD() 
    
        self.log.debug('CondorCREAMBatchSubmitPlugin.addJSD: Leaving.')

