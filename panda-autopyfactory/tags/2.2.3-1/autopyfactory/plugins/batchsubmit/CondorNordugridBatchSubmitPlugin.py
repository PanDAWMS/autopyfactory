#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCEBatchSubmitPlugin import CondorCEBatchSubmitPlugin 
import autopyfactory.utils as utils
from autopyfactory import jsd 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorNordugridBatchSubmitPlugin(CondorCEBatchSubmitPlugin):
    id = 'condornordugrid'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue):

        super(CondorNordugridBatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorNordugridBatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl=None):
        ''' 
        read the config loader object
        ''' 

        # Chosing the queue config object, depending on 
        if not qcl:
            qcl = self.apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condornordugrid', 'batchsubmit.condorce')
        valid = super(CondorNordugridBatchSubmitPlugin, self)._readconfig(newqcl) 
        if not valid:
            return False
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condornordugrid.gridresource', logger=self.log) 
            self.nordugridrsl = self._nordugridrsl(qcl)
            self.nordugridrsl_env = self._nordugridrsl_env(qcl)

            return True
        except:
            return False
            

    def _nordugridrsl(self, qcl):
        '''
        tries to build nordugrid_rsl line.
            -- nordugridrsl.XYZ
            -- nordugridrsl.nordugridrsl
            -- nordugridrsl.nordugridrsladd
        '''
 
        self.log.debug('_nordugridrsl: Starting.')
 
        out = ""
 
        optlist = []
        for opt in qcl.options(self.apfqname):
            if opt.startswith('nordugridrsl.') and\
                opt != 'nordugridrsl.nordugridrsl' and\
                opt != 'nordugridrsl.nordugridrsladd' and\
                not opt.startswith('nordugridrsl.addenv.'):
                    optlist.append(opt)
 
        rsl = qcl.generic_get(self.apfqname, 'nordugridrsl.nordugridrsl', logger=self.log)
        rsladd = qcl.generic_get(self.apfqname, 'nordugridrsl.nordugridrsladd', logger=self.log)

        if rsl:
            out = rsl 
        else:
                for opt in optlist:
                    key = opt.split('nordugridrsl.')[1]
                    value = qcl.generic_get(self.apfqname, opt, logger=self.log)
                    if value != "":
                            out += "(%s = %s)" %(key, value)
 
        if rsladd:
            out += rsladd
 
        self.log.debug('_nordugridrsl: Leaving with value = %s.' %out)
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
                value = qcl.generic_get(self.apfqname, opt, logger=self.log)
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
   
        self.JSD.add('grid_resource = nordugrid %s' %self.gridresource)

        nordugridrsl = "" 
        if self.nordugridrsl:
            nordugridrsl = self.nordugridrsl
        nordugridrsl += self.nordugridrsl_env
        self.JSD.add('nordugrid_rsl = %s' %nordugridrsl) 

        super(CondorNordugridBatchSubmitPlugin, self)._addJSD() 
    
        self.log.debug('CondorNordugridBatchSubmitPlugin.addJSD: Leaving.')

