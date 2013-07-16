#! /usr/bin/env python
#
import logging

from autopyfactory.interfaces import SchedInterface


class ReadySchedPlugin(SchedInterface):
    id = 'ready'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" % apfqueue.apfqname)
            try:
                self.offset = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.ready.offset', 'getint', default_value=0)
            except:
                pass 
                # Not mandatory
                
            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        """ 
        It just returns nb of Activated Jobs - nb of Pending Pilots
        """
        out = n
        self.log.debug('calcSubmitNum: Starting.')

        self.wmsinfo = self.apfqueue.wmsstatus_plugin.getInfo(maxtime = self.apfqueue.wmsstatusmaxtime)
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        if self.wmsinfo is None or self.batchinfo is None:
            self.log.warning("Missing info. wmsinfo is %s batchinfo is %s" % (self.wmsinfo, self.batchinfo))
            out = 0 
            msg = 'Invalid wmsinfo or batchinfo' 
        else:
            self.wmsqname = self.apfqueue.wmsqueue
            jobsdict = wmsinfo[self.wmsqname]
            self.log.info("WMS queue is %s" % self.wmsqname)
            if jobsdict is None:
                self.log.warning("Missing info. Jobsdict is None.")
                out = 0
                msg = 'Empty jobs dictionary wmsqueue %s' % self.wmsqname)
            else:
                (out, msg) = self._calc(input)
        return (out, msg)

    def _calc(self, input):
        '''
        algorithm 
        '''
        
        # initial default values. 
        activated_jobs = 0
        pending_pilots = 0
        running_pilots = 0

        self.log.debug("sitedict class is %s" % sitedict.__class__ )
        activated_jobs = self.wmsinfo[self.wmsqname].ready

        self.log.debug("batchinfo object is %s" % self.batchinfo)        
        self.log.debug("qi object for %s is %s" % (self.apfqueue.apfqname, self.batchinfo[self.apfqueue.apfqname]))
        pending_pilots = self.batchinfo[self.apfqueue.apfqname].pending
        running_pilots = self.batchinfo[self.apfqueue.apfqname].running

        self.log.debug("pending = %d running = %d offset = %d" % (pending_pilots, running_pilots, self.offset))
        
        out = max(0, ( activated_jobs - self.offset)  - pending_pilots )
        self.log.info('_calc() (input=%s; activated=%s; offset=%s pending=%s; running=%s;) : Return=%s' %(input,
                                                                                         activated_jobs,
                                                                                         self.offset, 
                                                                                         pending_pilots, 
                                                                                         running_pilots, 
                                                                                         out))
        msg = "ready=%d,offset=%d,pend=%d,ret=%d" % (activated_jobs, self.offset, pending_pilots, out)
        return (out,msg)
