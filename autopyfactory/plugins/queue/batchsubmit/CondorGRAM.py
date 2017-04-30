#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCE import CondorCE 
from autopyfactory import jsd 


class CondorGRAM(CondorCE):
   
    def __init__(self, apfqueue, config, section):

        qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgram', 'batchsubmit.condorce')    
        super(CondorGRAM, self).__init__(apfqueue, newqcl, section) 
        
        try:
            self.globus = self._globusrsl(apfqueue, qcl) 
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise

        self.log.info('CondorGRAM: Object initialized.')
  
    def _globusrsl(self, apfqueue, qcl):
        """
        tries to build globusrsl line.
        Entries have been renamed by the subplugins (e.g. CondorGT2), with new patterns:
            -- batchsubmit.condorgram.gram.XYZ
            -- batchsubmit.condorgram.gram.globusrsl
            -- batchsubmit.condorgram.gram.globusrsladd
        """
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
        """
        add things to the JSD object 
        """
    
        self.log.debug('CondorGRAM.addJSD: Starting.')
   
        # -- globusrsl -- 
        if self.globus:
            self.JSD.add('globusrsl', '%s' %self.globus)
        ###globusrsl = "globusrsl=(jobtype=%s)" %self.jobtype
        ###if self.queue:
        ###     globusrsl += "(queue=%s)" % self.queue
        ###self.JSD.add(globusrsl)

        # -- fixed stuffs --
        self.JSD.add('copy_to_spool', 'True')

        super(CondorGRAM, self)._addJSD() 
    
        self.log.debug('CondorGRAM.addJSD: Leaving.')
    
