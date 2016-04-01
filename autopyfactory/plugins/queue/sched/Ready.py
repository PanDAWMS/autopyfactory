#! /usr/bin/env python
#
import logging

from autopyfactory.interfaces import SchedInterface


class Ready(SchedInterface):
    id = 'ready'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" % apfqueue.apfqname)
            try:
                self.offset = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.ready.offset', 'getint', default_value=0)
                self.log.trace("SchedPlugin: offset = %d" % self.offset)
            except:
                pass 
                # Not mandatory
                
            self.log.trace("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex


    def calcSubmitNum(self, n=0):
        """ 
        It just returns nb of Activated Jobs - nb of Pending Pilots
        """
        out = n
        self.log.trace('Starting.')
        self.wmsqueueinfo = self.apfqueue.wmsstatus_plugin.getInfo(queue = self.apfqueue.wmsqueue, maxtime = self.apfqueue.wmsstatusmaxtime)
        self.queueinfo = self.apfqueue.batchstatus_plugin.getInfo(queue = self.apfqueue.apfqname, maxtime = self.apfqueue.batchstatusmaxtime)

        if self.wmsqueueinfo is None or self.queueinfo is None:
            self.log.warning("Missing info. wmsinfo is %s batchinfo is %s. Return=0" % (self.wmsqueueinfo, self.queueinfo))
            out = 0 
            msg = 'Invalid wmsinfo or batchinfo' 
        else:
            (out, msg) = self._calc(n)
        return (out, msg)

    def _calc(self, input):
        '''
        algorithm 
        '''
        
        # initial default values. 
        activated_jobs = 0
        pending_pilots = 0
        running_pilots = 0

        activated_jobs = self.wmsqueueinfo.ready    
        pending_pilots = self.queueinfo.pending
        running_pilots = self.queueinfo.running

        self.log.trace("pending = %s running = %s offset = %s" % (pending_pilots, running_pilots, self.offset))
        
        out = max(0, ( activated_jobs - self.offset)  - pending_pilots )

        msg = "Ready:in=%s;activated=%d,offset=%d,pending=%d;ret=%d" % (input, activated_jobs, self.offset, pending_pilots, out)
        self.log.info(msg)
        return (out,msg)
