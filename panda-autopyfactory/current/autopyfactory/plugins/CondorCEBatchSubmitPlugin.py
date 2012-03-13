#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin 
import jsd 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorCEBatchSubmitPlugin(CondorGridBatchSubmitPlugin):
   
    def __init__(self, apfqueue):

        super(CondorCEBatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorCEBatchSubmitPlugin: Object initialized.')
   
    def _readconfig(self, qcl):
        '''
        read the config loader object 
        '''

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorce', 'batchsubmit.condorgrid')
        valid = super(CondorCEBatchSubmitPlugin, self)._readconfig(newqcl)
        return valid

    def _addJSD(self):
        '''   
        add things to the JSD object
        '''   
 
        self.log.debug('CondorCEBatchSubmitPlugin.addJSD: Starting.')
   
        # -- fixed stuffs -- 
        #self.JSD.add('periodic_hold=GlobusResourceUnavailableTime =!= UNDEFINED &&(CurrentTime-GlobusResourceUnavailableTime>30)')
        #self.JSD.add('periodic_remove = (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > 3600) || (JobStatus == 1 && globusstatus =!= 1 && (CurrentTime - EnteredCurrentStatus) > 86400)')
        self.JSD.add('+Nonessential = True')

        super(CondorCEBatchSubmitPlugin, self)._addJSD() 
    
        self.log.debug('CondorCEBatchSubmitPlugin.addJSD: Leaving.')
    
