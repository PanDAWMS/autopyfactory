#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGRAMBatchSubmitPlugin import CondorGRAMBatchSubmitPlugin
from autopyfactory import jsd


class CondorGT2BatchSubmitPlugin(CondorGRAMBatchSubmitPlugin):
    id = 'condorgt2'
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
   
    def __init__(self, apfqueue):

        super(CondorGT2BatchSubmitPlugin, self).__init__(apfqueue) 
        self.log.info('CondorGT2BatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl=None):
        ''' 
        read the config loader object
        ''' 
        # Chosing the queue config object, depending on 
        if not qcl:
            qcl = self.apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorgt2', 'batchsubmit.condorgram').filterkeys('globusrsl.gram2', 'batchsubmit.condorgram.gram')
        valid = super(CondorGT2BatchSubmitPlugin, self)._readconfig(newqcl)
        if not valid:
            return False
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorgt2.gridresource') 
            return True
        except:
            return False


    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorGT2BatchSubmitPlugin.addJSD: Starting.')

        self.JSD.add('grid_resource', 'gt2 %s' % self.gridresource) 
        super(CondorGT2BatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorGT2BatchSubmitPlugin.addJSD: Leaving.')

