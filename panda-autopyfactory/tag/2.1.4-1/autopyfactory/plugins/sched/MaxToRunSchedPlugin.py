#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class MaxToRunSchedPlugin(SchedInterface):
    id = 'maxtorun'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" % apfqueue.apfqname)
            self.max_to_run = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.maxtorun.maximum', 'getint', logger=self.log)
            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, nsub=0):
        self.log.debug('calcSubmitNum: Starting with nsub=%s' %nsub)
        input = nsub
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)
        if self.batchinfo is None:
            self.log.warning("self.batchinfo is None!")
        else:
            pending_pilots = self.batchinfo[self.apfqueue.apfqname].pending
            running_pilots = self.batchinfo[self.apfqueue.apfqname].running
            all_pilots = pending_pilots + running_pilots
            if self.max_to_run:
                nsub = min(nsub, self.max_to_run - all_pilots)
            self.log.info('calcSubmitNum: (input=%s; pending=%s; running=%s): Return=%s' %(input, pending_pilots, running_pilots, nsub))
        return nsub
