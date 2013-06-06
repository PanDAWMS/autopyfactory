#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


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

    def calcSubmitNum(self, n=0):
        self.log.debug('calcSubmitNum: Starting with n=%s' %n)

        out = n
        msg = None

        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)
        if self.batchinfo is None:
            self.log.warning("self.batchinfo is None!")
        else:
            pending_pilots = self.batchinfo[self.apfqueue.apfqname].pending
            running_pilots = self.batchinfo[self.apfqueue.apfqname].running
            all_pilots = pending_pilots + running_pilots
            if self.max_to_run:
                out = min(n, self.max_to_run - all_pilots)
                msg = "MaxToRun: in=%s,max=%s,pend=%s,run=%s,ret=%s" % (n, 
                                                            self.max_to_run, 
                                                            pending_pilots, 
                                                            running_pilots, 
                                                            out)
            self.log.info('calcSubmitNum: (input=%s; pending=%s; running=%s): Return=%s' %(n, 
                                                                                           pending_pilots, 
                                                                                           running_pilots, 
                                                                                           out))

        return (out, msg) 
