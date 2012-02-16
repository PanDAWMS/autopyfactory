#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorBaseBatchSubmitPlugin import CondorBaseBatchSubmitPlugin 
import jsd 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorLocalBatchSubmitPlugin(CondorBaseBatchSubmitPlugin):
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
    
    def __init__(self, apfqueue, qcl=None):

        self.id = 'condorlocal'

        # Chosing the queue config object, depending on 
        # it was an input option or not.
        #       If it was passed as input option, then that is the config object. 
        #       If not, then it is extracted from the apfqueue object
        if not qcl:
            qcl = apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorlocal', 'batchsubmit.condorbase')
        super(CondorLocalBatchSubmitPlugin, self).__init__(apfqueue, newqcl) 

        try:
            self.executable = qcl.get(self.apfqname, 'executable')
            self.proxy = qcl.get(self.apfqname,'proxy')
            if self.proxy:
                self.x509userproxy = self.factory.proxymanager.getProxyPath(qcl.get(self.apfqname,'proxy'))
                self.log.debug('CondorLocalBatchSubmitPlugin. self.proxy is %s. Loaded path from proxymanager: %s' % (self.proxy, self.x509userproxy))
            else:
                self.x509userproxy = None
                self.log.debug('CondorLocalBatchSubmitPlugin. self.proxy is None. No proxy configured.')
            
            self.log.info('CondorLocalBatchSubmitPlugin: Object initialized.')
        except:
            self._valid = False

    
    def _addJSD(self):

        self.log.debug('CondorLocalBatchSubmitPlugin.addJSD: Starting.')

        super(CondorLocalBatchSubmitPlugin, self)._addJSD()

        # -- proxy path --
        if self.x509userproxy:
            self.JSD.add("x509userproxy=%s" % self.x509userproxy)

        # -- fixed stuffs -- 
        self.JSD.add("universe=vanilla")
        self.JSD.add('periodic_remove = (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > 3600) || (JobStatus == 1 && globusstatus =!= 1 && (CurrentTime - EnteredCurrentStatus) > 86400)')

        self.log.debug('CondorLocalBatchSubmitPlugin.addJSD: Leaving.')
    
