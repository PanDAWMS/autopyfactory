#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCEBatchSubmitPlugin import CondorCEBatchSubmitPlugin 


class CondorGRAMBatchSubmitPlugin(CondorCEBatchSubmitPlugin):
   
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.factory.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgram', 'batchsubmit.condorce')    
        super(CondorGRAMBatchSubmitPlugin, self).__init__(apfqueue, config=newqcl) 
        
        try:
            self.globus = self._globusrsl(apfqueue, qcl) 
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise

        self.log.info('CondorGRAMBatchSubmitPlugin: Object initialized.')
  
    def _globusrsl(self, apfqueue, qcl):
        '''
        tries to build globusrsl line.
        Entries have been renamed by the subplugins (e.g. CondorGT2), with new patterns:
            -- batchsubmit.condorgram.gram.XYZ
            -- batchsubmit.condorgram.gram.globusrsl
            -- batchsubmit.condorgram.gram.globusrsladd
        '''
        self.log.debug('Starting.')  # with new architecture there is no logger yet   
        globus = "" 
        optlist = []
        for opt in qcl.options(self.apfqname):
            if opt.startswith('batchsubmit.condorgram.gram.') and\
                opt != 'batchsubmit.condorgram.gram.globusrsl' and\
                opt != 'batchsubmit.condorgram.gram.globusrsladd':
                    optlist.append(opt)
        
        globusrsl = qcl.generic_get(self.apfqname, 'batchsubmit.condorgram.gram.globusrsl')
        globusrsladd = qcl.generic_get(self.apfqname, 'batchsubmit.condorgram.gram.globusrsladd')

        if globusrsl:
            globus = globusrsl
        else:
                for opt in optlist:
                    key = opt.split('batchsubmit.condorgram.gram.')[1]
                    value = qcl.generic_get(self.apfqname, opt)
                    if value != "":
                            globus += '(%s=%s)' %(key, value)
        
        if globusrsladd:
            globus += globusrsladd
        
        self.log.debug('Leaving with value = %s.' %globus)  # with new architecture there is no logger yet
        return globus
         
    def _addJSD(self):
        '''
        add things to the JSD object 
        '''
    
        self.log.debug('CondorGRAMBatchSubmitPlugin.addJSD: Starting.')
   
        # -- globusrsl -- 
        if self.globus:
            self.classads('GlobusRSL'] = %self.globus

        # -- fixed stuffs --
        self.classads('CopyToSpool', 'true')

        super(CondorGRAMBatchSubmitPlugin, self)._addJSD() 
    
        self.log.debug('CondorGRAMBatchSubmitPlugin.addJSD: Leaving.')
    
