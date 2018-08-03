#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class WeightedActivated(SchedInterface):
    id = 'weightedactivated'
    
    def __init__(self, apfqueue, config, section):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger('autopyfactory.sched.%s' %apfqueue.apfqname)

            # --- weights ---
            self.activated_w = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.weightedactivated.activated', 'getfloat', default_value=1.0)
            self.pending_w = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.weightedactivated.pending', 'getfloat', default_value=1.0)
            self.log.debug("SchedPlugin: weight values are activated_w=%s, pending_w=%s." %(self.activated_w, self.pending_w))

            self.log.debug("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        """ 
        It returns nb of Activated Jobs - nb of Pending Pilots
        But before making that calculation, it applies a scaling factor
        to both values: activated and pending
        """

        self.log.debug('Starting.')

        self.wmsinfo = self.apfqueue.wmsstatus_plugin.getInfo()
        #self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo()
        self.batchinfo = self.apfqueue.batchstatus_plugin.getOldInfo()

        if self.wmsinfo is None:
            self.log.warning("wsinfo is None!")
            out = self.default
            msg = "WeightedActivated:comment=no wmsinfo,in=%s,ret=%s" %(n, out)
        elif self.batchinfo is None:
            self.log.warning("self.batchinfo is None!")
            out = self.default            
            msg = "WeightedActivated:comment=no batchinfo,in=%,ret=%s" %(n, out)
        elif not self.wmsinfo.valid() and self.batchinfo.valid():
            out = self.default
            msg = "WeightedActivated:comment=no wms/batchinfo,in=%s,ret=%s" %(n, out)
            self.log.warn('a status is not valid, returning default = %s' %out)
        else:
            # Carefully get wmsinfo, activated. 
            self.wmsqueue = self.apfqueue.wmsqueue
            self.log.debug("wmsqueue is %s" % self.wmsqueue)

            (out, msg) = self._calc(n)
        return (out, msg)

    def _calc(self, n):
        
        # initial default values. 
        activated_jobs = 0
        pending_pilots = 0

        jobsinfo = self.wmsinfo.jobs
        self.log.debug("jobsinfo class is %s" % jobsinfo.__class__ )

        try:
            sitedict = jobsinfo[self.wmsqueue]
            self.log.debug("sitedict class is %s" % sitedict.__class__ )
            activated_jobs = sitedict.ready
        except KeyError:
            # This is OK--it just means no jobs in any state at the wmsqueue. 
            self.log.error("wmsqueue %s not present in jobs info from WMS" % self.wmsqueue)
            activated_jobs = 0
        try:
            pending_pilots = self.batchinfo[self.apfqueue.apfqname].pending  # using the new info objects
        except KeyError:
            # This is OK--it just means no jobs. 
            pass
        except KeyError:
            # This is OK--it just means no jobs. 
            pass

        # correct values based on weights
        activated_jobs_w = int(activated_jobs * self.activated_w)
        pending_pilots_w = int(pending_pilots * self.pending_w)

        out = max(0, activated_jobs_w - pending_pilots_w)

        msg = "WeightedActivated:in=%s,activated=%s,weightedactivated=%s,pending=%s,weightedpending=%s,ret=%s" %(n, activated_jobs, activated_jobs_w, pending_pilots, pending_pilots_w, out)
        self.log.info(msg)
        return (out, msg)
