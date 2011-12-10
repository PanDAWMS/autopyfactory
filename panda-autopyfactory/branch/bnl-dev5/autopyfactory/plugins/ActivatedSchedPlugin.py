#! /usr/bin/env python
#

from autopyfactory.factory import SchedInterface
import logging

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class SchedPlugin(SchedInterface):
    
    #def __init__(self, apfqueue):
    def initialize(self, apfqueue):
        self.apfqueue = apfqueue                
        self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)
        self.max_jobs_torun = None
        self.max_pilots_per_cycle = None
        self.min_pilots_per_cycle = None
        self.max_pilots_pending = None
        
        # A default value is required. 
        self.default = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.activated.default')    
        self.log.debug('SchedPlugin: default = %s' %self.default)
        
        if self.apfqueue.qcl.has_option(self.apfqueue.apfqname, 'sched.activated.max_jobs_torun'):
            self.max_jobs_torun = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.activated.max_jobs_torun')
            self.log.debug('SchedPlugin: max_jobs_torun = %s' %self.max_jobs_torun)
 
        if self.apfqueue.qcl.has_option(self.apfqueue.apfqname, 'sched.activated.max_pilots_per_cycle'):
            self.max_pilots_per_cycle = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.activated.max_pilots_per_cycle')
            self.log.debug('SchedPlugin: max_pilots_per_cycle = %s' %self.max_pilots_per_cycle)

        if self.apfqueue.qcl.has_option(self.apfqueue.apfqname, 'sched.activated.min_pilots_per_cycle'):
            self.min_pilots_per_cycle = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.activated.min_pilots_per_cycle')
            self.log.debug('SchedPlugin: there is a MIN_PILOTS_PER_CYCLE number setup to %s' %self.min_pilots_per_cycle)
        
        if self.apfqueue.qcl.has_option(self.apfqueue.apfqname, 'sched.activated.max_pilots_pending'):
            self.max_pilots_pending = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.activated.max_pilots_pending')
            self.log.debug('SchedPlugin: there is a MAX_PILOTS_PENDING number setup to %s' %self.max_pilots_pending)   

        self.log.info("SchedPlugin: Object initialized.")

    def calcSubmitNum(self):
        """ 
        By default, returns nb of Activated Jobs - nb of Pending Pilots

        But, if max_jobs_running is defined, 
        it can impose a limit on the number of new pilots,
        to prevent passing that limit on max nb of running jobs.

        If there is a max_pilots_per_cycle defined, 
        it can impose a limit too.

        If there is a min_pilots_per_cycle defined, 
        and the final decission was a lower number, 
        then this min_pilots_per_cycle is the number of pilots 
        to be submitted.
        """
        self.log.debug('calcSubmitNum: Starting.')

        # initial default values. 
        activated_jobs = 0
        pending_pilots = 0
        running_pilots = 0

        wmsinfo = self.apfqueue.wmsstatus.getInfo(maxtime = self.apfqueue.wmsstatusmaxtime)
        batchinfo = self.apfqueue.batchstatus.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        if wmsinfo is None:
            self.log.warning("wsinfo is None!")
            out = self.default
        elif batchinfo is None:
            self.log.warning("batchinfo is None!")
            out = self.default            
        elif not wmsinfo.valid() and batchinfo.valid():
            out = self.default
            self.log.warn('calcSubmitNum: a status is not valid, returning default = %s' %out)
        else:
            # Carefully get wmsinfo, activated. 
            siteid = self.apfqueue.siteid
            self.log.debug("Siteid is %s" % siteid)
            jobsinfo = wmsinfo.jobs
            self.log.debug("jobsinfo class is %s" % jobsinfo.__class__ )
            try:
                sitedict = jobsinfo[siteid]
                self.log.debug("sitedict class is %s" % sitedict.__class__ )
                #activated_jobs = sitedict['activated']
                activated_jobs = sitedict.ready
            except KeyError:
                # This is OK--it just means no jobs in any state at the siteid. 
                self.log.error("siteid: %s not present in jobs info from WMS" % siteid)
                activated_jobs = 0
           
            try:
                pending_pilots = batchinfo[self.apfqueue.apfqname].pending  # using the new info objects
            except KeyError:
                                # This is OK--it just means no jobs. 
                pass
            
            try:        
                running_pilots = batchinfo[self.apfqueue.apfqname].running # using the new info objects
            except KeyError:
                # This is OK--it just means no jobs. 
                pass

            all_pilots = pending_pilots + running_pilots
            out = max(0, activated_jobs - all_pilots)
            
            if self.max_jobs_torun: 
                out = min(out, self.max_jobs_torun - all_pilots)
           
            if self.max_pilots_per_cycle:
                out = min(out, self.max_pilots_per_cycle)

            if self.min_pilots_per_cycle:
                out = max(out, self.min_pilots_per_cycle)

            if self.max_pilots_pending:
                out = min(out, self.max_pilots_pending - pending_pilots)

            # Catch all to prevent negative numbers
            if out < 0:
                out = 0
            
        self.log.info('calcSubmitNum (activated=%s; pending=%s; running=%s;) : Return=%s' %(activated_jobs, 
                                                                                             pending_pilots, 
                                                                                             running_pilots, 
                                                                                             out))
        return out
