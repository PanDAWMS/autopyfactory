#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorBase import CondorBase 
from autopyfactory import jsd

class CondorLocal(CondorBase):
    id = 'condorlocal'
    """
    This class is expected to have separate instances for each PandaQueue object. 
    """
    
    def __init__(self, apfqueue, config, section):

        qcl = config             

        newqcl = qcl.clone().filterkeys('batchsubmit.condorlocal', 'batchsubmit.condorbase')           
        super(CondorLocal, self).__init__(apfqueue, newqcl, section) 
        
        self.x509userproxy = None
        plist = qcl.generic_get(self.apfqname, 'batchsubmit.condorlocal.proxy')
        # This is alist of proxy profile names specified in proxy.conf
        # We will only attempt to derive proxy file path during submission
        if plist:
            self.proxylist = [x.strip() for x in plist.split(',')]
        else:
            self.proxylist = None
           
        self.log.info('CondorLocal: Object initialized.')

    def _getX509Proxy(self):
        """
        
        """
        self.log.debug("Determining proxy, if necessary. Profile: %s" % self.proxylist)
        if self.proxylist:
            self.x509userproxy = self.factory.authmanager.getProxyPath(self.proxylist)
        else:
            self.x509userproxy = None
            self.log.debug("No proxy profile defined.")       
       
    def _addJSD(self):
        """
        add things to the JSD object
        """
        self.log.debug('CondorLocal.addJSD: Starting.')
        if self.x509userproxy:
            self.JSD.add("x509userproxy", "%s" % self.x509userproxy)
        self.JSD.add("universe", "vanilla")
        self.JSD.add("should_transfer_files", "IF_NEEDED")
        self.JSD.add('+TransferOutput', '""')
        super(CondorLocal, self)._addJSD()
        self.log.debug('CondorLocal.addJSD: Leaving.')
    
