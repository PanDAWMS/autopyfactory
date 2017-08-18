#!/bin/env python
#
# AutoPyfactory batch plugin for WorkQueue running on a local Condor
#

import os
import sys

from CondorLocal import CondorLocal 
from autopyfactory import jsd

#### READ FROM WorkQueueCatalog, but how?
try:
    import infoclient
except ImportError:
    pass
####

class WorkQueueCondor(CondorLocal):
    id = 'workqueuecondor'
    
    def __init__(self, apfqueue, config, section):

        qcl = config             

        newqcl = qcl.clone()

        super(WorkQueueCondor, self).__init__(apfqueue, newqcl, section) 

        self.arguments_original  = self.arguments
        self.executable = os.path.expandvars(self.executable)

        ############################ READ FROM WorkQueueCatalog, BUT HOW?
        use_infoclient = self.apfqueue.qcl.generic_get(self.apfqname, 'workqueue.infoclient.enabled', default_value = False)
        self.catalog    = None
        self.infoclient = None
        if use_infoclient:
            if 'infoclient' in sys.modules:
                self.requestid  = self.apfqueue.fcl.generic_get('Factory', 'requestid')
                self.infoclient = infoclient.InfoClient(self.apfqueue.fcl)
            else:
                raise Exception('infoclient requested, but module was not loaded.')
        else:
            self.catalog = self.apfqueue.qcl.generic_get(self.apfqname, 'workqueue.catalog', None) 
            if not self.catalog:
                self.catalog = 'catalog.cse.nd.edu:9097'
        ############################

        self.log.info('WorkQueueCondor: Object initialized.')

    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.trace('WorkQueueCondor.addJSD: Starting.')
        
        super(WorkQueueCondor, self)._addJSD()

        self.JSD.add("universe",                "vanilla")
        self.JSD.add("should_transfer_files",   "YES")
        self.JSD.add("when_to_transfer_output", "ON_EXIT")
        self.JSD.add("transfer_executable",     "YES")
        self.JSD.add("copy_to_spool",           "YES")

        self.log.trace('WorkQueueCondor.addJSD: Leaving.')
        
    def submit(self, n):

        ############################ READ FROM WorkQueueCatalog, instead of contacting the info client instead, BUT HOW?
        #wmsqueueinfo = self.apfqueue.wmsstatus_plugin.getInfo(queue = self.apfqueue.wmsqueue) does not quite work

        if self.infoclient:
            self.catalog = None
            info = None

            try:
                info = self.infoclient.getbranch('runtime', self.requestid, 'services', 'cctools-catalog-server')
            except Exception, e:
                # We check for empty catalog below
                pass

            if info:
                try:
                    self.catalog = info['hostname'] + ':' + info['port']
                except KeyError:
                    pass
        ############################ END OF READ FROM WorkQueueCatalog, BUT HOW?
        
        m = n
        if not self.catalog:
            m = 0

        if n > 0 and m == 0:
            self.log.info('No catalog information found. No jobs will be submitted.')
        else:
            self.arguments = self.arguments_original + ' --catalog ' + self.catalog

        return super(WorkQueueCondor, self).submit(m)

