#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCE import CondorCE 
import autopyfactory.utils as utils
from autopyfactory import jsd 


class CondorNordugrid(CondorCE):
    id = 'condornordugrid'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue, config, section):

        qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condornordugrid', 'batchsubmit.condorce')
        super(CondorNordugrid, self).__init__(apfqueue, config=newqcl, section) 
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condornordugrid.gridresource') 
            self.nordugridrsl = self._nordugridrsl(qcl)
            self.nordugridrsladdenv = self._nordugridrsl_addenv(qcl)
            #self.nordugridrsl_env = self._nordugridrsl_env(qcl)
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        
        self.log.info('CondorNordugrid: Object initialized.')
                 

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

    def _nordugridrsl_addenv(self, qcl):
        # the next is for tagas like 
        #       ('RUCIO_ACCOUNT' 'pilot')
        # inside the environment tag
        
        out= " "
        for opt in qcl.options(self.apfqname):
            if opt.startswith('nordugridrsl.addenv.'):
                key = opt.split('nordugridrsl.addenv.')[1]
                value = qcl.generic_get(self.apfqname, opt)
                if value != "":
                    out += "('%s' '%s')" %(key, value) 
                           
        self.log.debug('Leaving with value = %s.' %out)
        return out

    def _nordugridrsl_env(self):

        # open the environment tag
        nordugridrsl_env = "( "
        
        nordugridrsl_env = " (environment = " 
        nordugridrsl_env += "('APFFID' '%s') " % self.factoryid
        nordugridrsl_env += "('PANDA_JSID' '%s') " % self.factoryid
        nordugridrsl_env += "('APFCID' '$(Cluster).$(Process)') " 
        nordugridrsl_env += " ('GTAG' '%s/$(Cluster).$(Process).out') " % self.logUrl
        nordugridrsl_env += "('APFMON' '%s') " % self.monitorurl
        nordugridrsl_env += "('FACTORYQUEUE' '%s') " % self.apfqname
        nordugridrsl_env += "('FACTORYUSER' '%s') " % self.factoryuser
        nordugridrsl_env += " %s " % self.nordugridrsladdenv

        # closing the environment tag
        nordugridrsl_env += ") "

        return nordugridrsl_env


    def _addJSD(self):
        '''
        add things to the JSD object
        '''
        self.log.debug('CondorNordugrid.addJSD: Starting.')
   
        self.JSD.add('grid_resource', 'nordugrid %s' %self.gridresource)

        nordugridrsl = "" 
        if self.nordugridrsl:
            nordugridrsl = self.nordugridrsl
            nordugridrsl += self._nordugridrsl_env()
        
        self.JSD.add('nordugrid_rsl', '%s' % nordugridrsl) 

        super(CondorNordugrid, self)._addJSD() 
    
        self.log.debug('CondorNordugrid.addJSD: Leaving.')

