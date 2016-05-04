#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCEBatchSubmitPlugin import CondorCEBatchSubmitPlugin 
import autopyfactory.utils as utils
from autopyfactory import jsd 


class CondorNordugridBatchSubmitPlugin(CondorCEBatchSubmitPlugin):
    id = 'condornordugrid'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.factory.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condornordugrid', 'batchsubmit.condorce')
        super(CondorNordugridBatchSubmitPlugin, self).__init__(apfqueue, config=newqcl) 
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condornordugrid.gridresource') 
            self.nordugridrsl = self._nordugridrsl(qcl)
            self.nordugridrsl_env = self._nordugridrsl_env(qcl)
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        
        self.log.info('CondorNordugridBatchSubmitPlugin: Object initialized.')
                 

    def _nordugridrsl(self, qcl):
        '''
        tries to build nordugrid_rsl line.
            -- nordugridrsl.XYZ
            -- nordugridrsl.nordugridrsl
            -- nordugridrsl.nordugridrsladd
        '''
        self.log.debug('Starting.')
        out = ""
        optlist = []
        for opt in qcl.options(self.apfqname):
            if opt.startswith('nordugridrsl.') and\
                opt != 'nordugridrsl.nordugridrsl' and\
                opt != 'nordugridrsl.nordugridrsladd' and\
                not opt.startswith('nordugridrsl.addenv.'):
                    optlist.append(opt)
 
        rsl = qcl.generic_get(self.apfqname, 'nordugridrsl.nordugridrsl')
        rsladd = qcl.generic_get(self.apfqname, 'nordugridrsl.nordugridrsladd')

        if rsl:
            out = rsl 
        else:
                for opt in optlist:
                    key = opt.split('nordugridrsl.')[1]
                    value = qcl.generic_get(self.apfqname, opt)
                    if value != "":
                            out += "(%s = %s)" %(key, value)
 
        if rsladd:
            out += rsladd
 
        self.log.debug('Leaving with value = %s.' %out)
        return out 

    def _nordugridrsl_env(self, qcl):

        nordugridrsl_env = " (environment = " 
        nordugridrsl_env += "('APFFID' '%s') " % self.factoryid
        nordugridrsl_env += "('PANDA_JSID' '%s') " % self.factoryid
        nordugridrsl_env += "('GTAG' '%s/$(Cluster).$(Process).out') " % self.logUrl
        nordugridrsl_env += "('APFCID' '$(Cluster).$(Process)') " 
        nordugridrsl_env += "('APFMON' '%s') " % self.monitorurl
        nordugridrsl_env += "('FACTORYQUEUE' '%s') " % self.apfqname
        nordugridrsl_env += "('FACTORYUSER' '%s') " % self.factoryuser

        # the next is for tagas like 
        #       ('RUCIO_ACCOUNT' 'pilot')
        # inside the environment tag
        for opt in qcl.options(self.apfqname):
            if opt.startswith('nordugridrsl.addenv.'):
                key = opt.split('nordugridrsl.addenv.')[1]
                value = qcl.generic_get(self.apfqname, opt)
                if value != "":
                    nordugridrsl_env += "('%s' '%s')" %(key, value)

        # closing the environment tag
        nordugridrsl_env += ") "

        return nordugridrsl_env


    def _addJSD(self):
        '''
        add things to the JSD object
        '''
    
        self.log.debug('CondorNordugridBatchSubmitPlugin.addJSD: Starting.')
   
        self.JSD.add('grid_resource', 'nordugrid %s' %self.gridresource)

        nordugridrsl = "" 
        if self.nordugridrsl:
            nordugridrsl = self.nordugridrsl
        nordugridrsl += self.nordugridrsl_env
        self.JSD.add('nordugrid_rsl', '%s' %nordugridrsl) 

        super(CondorNordugridBatchSubmitPlugin, self)._addJSD() 
    
        self.log.debug('CondorNordugridBatchSubmitPlugin.addJSD: Leaving.')

