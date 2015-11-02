#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorBaseBatchSubmitPlugin import CondorBaseBatchSubmitPlugin 
from autopyfactory.apfexceptions import InvalidProxyFailure
from autopyfactory import jsd 


class CondorGridBatchSubmitPlugin(CondorBaseBatchSubmitPlugin):
   
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgrid', 'batchsubmit.condorbase')
        super(CondorGridBatchSubmitPlugin, self).__init__(apfqueue, newqcl) 
        
        # ---- proxy management ------
        self.x509userproxy = None
        self.proxylist = None
        try:
            plist = qcl.generic_get(self.apfqname, 'batchsubmit.condorgrid.proxy')
            # This is alist of proxy profile names specified in proxy.conf
            # We will only attempt to derive proxy file path during submission
            if plist:
                self.proxylist = [x.strip() for x in plist.split(',')]
            self._getX509Proxy()
        except InvalidProxyFailure, ipf:
            self.log.error('Unable to get valid proxy file.')
            raise
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        except:
            raise

        self.log.info('CondorGridBatchSubmitPlugin: Object initialized.')


    def _getX509Proxy(self):
        '''
        uses proxymanager to find out the path to the X509 file
        '''
    
        self.log.debug("Determining proxy, if necessary. Profile: %s" % self.proxylist)
        if self.proxylist:
            self.x509userproxy = self.factory.proxymanager.getProxyPath(self.proxylist)
        else:
            self.log.debug("No proxy profile defined.")


    def _addJSD(self):
        '''   
        add things to the JSD object
        '''   
 
        self.log.debug('CondorGridBatchSubmitPlugin.addJSD: Starting.')
   
        self.JSD.add("universe", "grid")
        # -- proxy path --
        if self.x509userproxy:
            self.JSD.add("x509userproxy", "%s" % self.x509userproxy)
        else:
            self.log.critical('no x509 proxy found. Aborting')
            raise Exception 

        super(CondorGridBatchSubmitPlugin, self)._addJSD()
    
        self.log.debug('CondorGridBatchSubmitPlugin.addJSD: Leaving.')
    