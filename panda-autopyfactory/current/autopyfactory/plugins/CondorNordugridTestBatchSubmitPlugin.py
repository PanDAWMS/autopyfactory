#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorCEBatchSubmitPlugin import CondorCEBatchSubmitPlugin 
import autopyfactory.utils as utils
import jsd 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorNordugridTestBatchSubmitPlugin(CondorCEBatchSubmitPlugin):
    id = 'condornordugridtest'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue):

        super(CondorNordugridTestBatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorNordugridTestBatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl=None):
        ''' 
        read the config loader object
        ''' 

        # Chosing the queue config object, depending on 
        if not qcl:
            qcl = self.apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condornordugrid', 'batchsubmit.condorce')
        valid = super(CondorNordugridTestBatchSubmitPlugin, self)._readconfig(newqcl) 
        if not valid:
            return False
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condornordugrid.gridresource', logger=self.log) 
            self.nordugridrsl = _nordugridrsl(qcl)

            return True
        except:
            return False
            

    def _addJSD(self):
        '''
        add things to the JSD object
        '''
    
        self.log.debug('CondorNordugridTestBatchSubmitPlugin.addJSD: Starting.')
   
        self.JSD.add('grid_resource = nordugrid %s' %self.gridresource)

        nordugridrsl_env = " (environment = " 
        nordugridrsl_env += "('APFFID' '%s') " % self.factoryid
        nordugridrsl_env += "('PANDA_JSID' '%s') " % self.factoryid
        nordugridrsl_env += "('GTAG' '%s/$(Cluster).$(Process).out') " % self.logUrl
        nordugridrsl_env += "('APFCID' '$(Cluster).$(Process)') " 
        nordugridrsl_env += "('APFMON' '%') " % self.monitorurl
        nordugridrsl_env += "('FACTORYQUEUE' '%') " % self.apfqname
        nordugridrsl_env += ") "

        if self.nordugridrsl:
            nordugridrsl = self.nordugridrsl
            nordugridrsl += nordugridrsl_env
            self.JSD.add('nordugrid_rsl = %s' %nordugridrsl) 


        super(CondorNordugridTestBatchSubmitPlugin, self)._addJSD() 
    
        self.log.debug('CondorNordugridTestBatchSubmitPlugin.addJSD: Leaving.')


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
                opt != 'nordugridrsl.nordugridrsladd':
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
                            nordugrid += "('%s' '%s')" %(key, value)
 
        if rsladd:
            out += rsladd
 
        self.log.debug('_nordugridrsl: Leaving with value = %s.' %out)
        return out 
