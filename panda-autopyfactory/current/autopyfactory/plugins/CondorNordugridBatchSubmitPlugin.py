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
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorNordugridBatchSubmitPlugin(CondorCEBatchSubmitPlugin):
    id = 'condornordugrid'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue):

        super(CondorNordugridBatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorNordugridBatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl=None):
        ''' 
        read the config loader object
        ''' 

        # Chosing the queue config object, depending on 
        if not qcl:
            qcl = self.apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condornordugrid', 'batchsubmit.condorce')
        valid = super(CondorNordugridBatchSubmitPlugin, self)._readconfig(newqcl) 
        if not valid:
            return False
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condornordugrid.gridresource', logger=self.log) 
            self.nordugridrsl = qcl.generic_get(self.apfqname, 'batchsubmit.condornordugrid.nordugridrsl', default_value=None, logger=self.log) 

            return True
        except:
            return False
            

    def _addJSD(self):
        '''
        add things to the JSD object
        '''
    
        self.log.debug('CondorNordugridBatchSubmitPlugin.addJSD: Starting.')
   
        self.JSD.add('grid_resource = nordugrid %s' %self.gridresource)
        if self.nordugridrsl:
            self.JSD.add('nordugrid_rsl = %s' %self.nordugridrsl) 

        super(CondorNordugridBatchSubmitPlugin, self)._addJSD() 
    
        self.log.debug('CondorNordugridBatchSubmitPlugin.addJSD: Leaving.')

