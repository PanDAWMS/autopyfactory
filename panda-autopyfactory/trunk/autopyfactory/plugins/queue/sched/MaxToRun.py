#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class MaxToRun(SchedInterface):
    id = 'maxtorun'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" % apfqueue.apfqname)
            self.max_to_run = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.maxtorun.maximum', 'getint')
            self.log.debug("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        self.log.debug('Starting with n=%s' %n)

        out = n
        msg = None

        batchinfo = self.apfqueue.batchstatus_plugin.getInfo(queue=self.apfqueue.apfqname, maxtime = self.apfqueue.batchstatusmaxtime)
        if batchinfo is None:
            out = n
            msg = "MaxToRun:No batchinfo."
            self.log.warning("self.batchinfo is None!")
        else:
            pending_pilots = batchinfo.pending
            running_pilots = batchinfo.running
            all_pilots = pending_pilots + running_pilots
            if self.max_to_run is not None:
                out = min(n, self.max_to_run - all_pilots)
                msg = "MaxToRun:in=%s;maxtorun=%s,pending=%s,running=%s;ret=%s" % (n, 
                                                            self.max_to_run, 
                                                            pending_pilots, 
                                                            running_pilots, 
                                                            out)
            self.log.info('(input=%s; pending=%s; running=%s): Return=%s' %(n, 
                                                                                           pending_pilots, 
                                                                                           running_pilots, 
                                                                                           out))
        return (out, msg) 
