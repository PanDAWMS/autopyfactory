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
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorCEBatchSubmitPlugin(CondorGridBatchSubmitPlugin):
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, qcl):

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorce', 'batchsubmit.condorgrid')
        super(CondorCEBatchSubmitPlugin, self).__init__(apfqueue, newqcl) 

        try:
            self.x509userproxy = self.factory.proxymanager.getProxyPath(qcl.get(self.apfqname,'proxy'))
            self.log.info('CondorCEBatchSubmitPlugin: Object initialized.')
        except:
            self._valid = False
   

    def _addJSD(self):
    
        self.log.debug('CondorCEBatchSubmitPlugin.addJSD: Starting.')
   
        super(CondorCEBatchSubmitPlugin, self)._addJSD() 
    
        # -- proxy path --
        self.JSD.add("x509userproxy=%s" % self.x509userproxy) 
       
        # -- fixed stuffs -- 
        self.JSD.add('periodic_hold=GlobusResourceUnavailableTime =!= UNDEFINED &&(CurrentTime-GlobusResourceUnavailableTime>30)')
        self.JSD.add('periodic_remove = (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > 3600) || (JobStatus == 1 && globusstatus =!= 1 && (CurrentTime - EnteredCurrentStatus) > 86400)')
        self.JSD.add('+Nonessential = True')
    
        self.log.debug('CondorCEBatchSubmitPlugin.addJSD: Leaving.')
    
