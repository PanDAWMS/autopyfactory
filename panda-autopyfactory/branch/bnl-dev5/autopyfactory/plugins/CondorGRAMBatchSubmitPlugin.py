#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCEBatchSubmitPlugin import CondorCEBatchSubmitPlugin 
import jsd 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorGRAMBatchSubmitPlugin(CondorCEBatchSubmitPlugin):
   
    def __init__(self, apfqueue):

        super(CondorGRAMBatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorGRAMBatchSubmitPlugin: Object initialized.')
  
    def _readconfig(self, qcl):
        '''
        read the config loader object
        '''

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgram', 'batchsubmit.condorce')
        super(CondorGRAMBatchSubmitPlugin, self)._readconfig(newqcl) 

        #self.queue = None
        #if qcl.has_option(self.apfqname,'batchsubmit.condorgram.queue'):
        #    self.queue = qcl.get(self.apfqname, 'batchsubmit.condorgram.queue')
        self.queue = qcl.generic_get(self.apfqname, 'batchsubmit.condorgram.queue', logger=self.log)
         
    def _addJSD(self):
        '''
        add things to the JSD object 
        '''
    
        self.log.debug('CondorGRAMBatchSubmitPlugin.addJSD: Starting.')
   
        super(CondorGRAMBatchSubmitPlugin, self)._addJSD() 
    
        # -- globusrsl -- 
        globusrsl = "globusrsl=(jobtype=single)"
        if self.queue:
             globusrsl += "(queue=%s)" % self.queue
        self.JSD.add(globusrsl)

        # -- fixed stuffs --
        self.JSD.add('copy_to_spool = false')
    
        self.log.debug('CondorGRAMBatchSubmitPlugin.addJSD: Leaving.')
    
