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
__version__ = "2.1.0"
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
        valid = super(CondorGRAMBatchSubmitPlugin, self)._readconfig(newqcl) 
        if not valid:
            return False
        try:
            self.globus = self._globusrsl()
            return True
        except:
            return False

    def _globusrsl(self):
        '''
        tries to build globusrsl line.
        Entries have been renamed by the subplugins (e.g. CondorGT2), with new patterns:
            -- batchsubmit.condorgram.gram.XYZ
            -- batchsubmit.condorgram.gram.globusrsl
            -- batchsubmit.condorgram.gram.globusrsladd
        '''

        self.globus = None

        optlist = []
        for opt in qcl.options(self.apfqname):
            if opt.startswith('batchsubmit.condorgram.gram.') and\
                opt != 'batchsubmit.condorgram.gram.globusrsl' and\
                opt != 'batchsubmit.condorgram.gram.globusrsladd':
                    optlist.append(opt)
        
        globusrsl = q.generic_get(self.apfqname, 'batchsubmit.condorgram.gram.globusrsl')
        globusrsladd = q.generic_get(self.apfqname, 'batchsubmit.condorgram.gram.globusrsladd')

        if globusrsl:
            globus = globusrsl
        else:
                for opt in lopts:
                    key = opt.split('batchsubmit.condorgram.gram.')[1]
                    value = q.generic_get(self.apfqname, opt)
                    if value != "":
                            globus += '(%s=%s)' %(key, value)
        
        if globusrsladd:
            globus += globusrsladd
        
        return globus
         
    def _addJSD(self):
        '''
        add things to the JSD object 
        '''
    
        self.log.debug('CondorGRAMBatchSubmitPlugin.addJSD: Starting.')
   
        # -- globusrsl -- 
        if self.globus:
            self.JSD.add('globusrsl=%s' %self.globus)
        ###globusrsl = "globusrsl=(jobtype=%s)" %self.jobtype
        ###if self.queue:
        ###     globusrsl += "(queue=%s)" % self.queue
        ###self.JSD.add(globusrsl)

        # -- fixed stuffs --
        self.JSD.add('copy_to_spool = false')

        super(CondorGRAMBatchSubmitPlugin, self)._addJSD() 
    
        self.log.debug('CondorGRAMBatchSubmitPlugin.addJSD: Leaving.')
    
