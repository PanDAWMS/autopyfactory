#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class Fillactivated(SchedInterface):
    id = 'fillactivated'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            # A default value is required. 
            self.default = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.fillactivated.default', 'getint', default_value=0)    
            self.log.debug('SchedPlugin: default = %s' %self.default)
            
            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        """ 
        returns nb of Activated Jobs - nb of Pending Pilots
        """
        self.log.debug('Starting.')

        self.wmsinfo = self.apfqueue.wmsstatus_plugin.getInfo()
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo()

        if self.wmsinfo is None:
            self.log.warning("wmsinfo is None!")
            out = self.default
            msg = "Invalid wmsinfo"
        elif self.batchinfo is None:
            self.log.warning("self.batchinfo is None!")
            out = self.default            
            msg = "Invalid batchinfo"
        elif not self.wmsinfo.valid() and self.batchinfo.valid():
            out = self.default
            msg = "Invalid wms/batchinfo"
            self.log.warn('a status is not valid, returning default = %s' %out)
        else:
            # Carefully get wmsinfo, activated. 
            self.wmsqueue = self.apfqueue.wmsqueue
            self.log.info("Siteid is %s" % self.wmsqueue)

        (out, msg) = _calc_online(n) 
        return (out, msg)

    def _calc_online(self, n):
        '''
        algorithm when wmssite is in online mode
        '''
        
        # initial default values. 
        activated_jobs = 0
        pending_pilots = 0
        running_pilots = 0

        jobsinfo = self.wmsinfo.jobs
        self.log.debug("jobsinfo class is %s" % jobsinfo.__class__ )

        try:
            sitedict = jobsinfo[self.wmsqueue]
            self.log.debug("sitedict class is %s" % sitedict.__class__ )
            #activated_jobs = sitedict['activated']
            activated_jobs = sitedict.ready
        except KeyError:
            # This is OK--it just means no jobs in any state at the wmsqueue. 
            self.log.error("wmsqueue: %s not present in jobs info from WMS" % self.wmsqueue)
            activated_jobs = 0
        try:
            pending_pilots = self.batchinfo[self.apfqueue.apfqname].pending  # using the new info objects
        except KeyError:
            # This is OK--it just means no jobs. 
            pass
        try:        
            running_pilots = self.batchinfo[self.apfqueue.apfqname].running # using the new info objects
        except KeyError:
            # This is OK--it just means no jobs. 
            pass

        all_pilots = pending_pilots + running_pilots

        out = max(0, activated_jobs - pending_pilots)
        
        self.log.info('activated=%s; pending=%s; running=%s; Return=%s' %(activated_jobs, 
                                                                          pending_pilots, 
                                                                          running_pilots, 
                                                                          out))
        msg = "Fillactivated:in=%s,activated=%s,pending=%s,running=%s,ret=%s" %(n, activated_jobs, pending_pilots, running_pilots, out)
        return (out, msg)
