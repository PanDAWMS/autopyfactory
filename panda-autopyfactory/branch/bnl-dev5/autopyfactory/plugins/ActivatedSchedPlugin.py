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
    
    def __init__(self, wmsqueue):
        self.wmsqueue = wmsqueue                
        self.log = logging.getLogger("main.schedplugin[%s]" %wmsqueue.apfqueue)
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

        # giving an initial value to some variables
        # to prevent the logging from crashing
        activated_jobs = 0
        pending_pilots = 0
        running_pilots = 0

        wmsinfo = self.wmsqueue.wmsstatus.getInfo(maxtime = self.wmsqueue.wmsstatusmaxtime)
        batchinfo = self.wmsqueue.batchstatus.getInfo(maxtime = self.wmsqueue.batchstatusmaxtime)

        if wmsinfo is None or batchinfo is None:
            self.log.warning("wsinfo or batchinfo is None!")
            out = 0
        elif not wmsinfo.valid() and batchinfo.valid():
            out = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'sched.activated.default')
            self.log.warn('calcSubmitNum: a status is not valid, returning default = %s' %out)
        else:
            # NOTE: This could change to 
            # nbjobs = wmsinfo.jobs[self.wmsqueue.apfqueue].activated      
            # if we regularize the *Info object interfaces. 
            #
            # Carefully get wmsinfo, activated. 
            siteid = self.wmsqueue.siteid
            self.log.debug("Siteid is %s" % siteid)
            jobsinfo = wmsinfo.jobs
            self.log.debug("jobsinfo class is %s" % jobsinfo.__class__ )
            try:
                sitedict = jobsinfo[siteid]
                self.log.debug("sitedict class is %s" % sitedict.__class__ )
                activated_jobs = sitedict['activated']
            
            except KeyError:
                self.log.error("siteid: %s not present in jobs info from WMS" % siteid)

            #activate_jobs = wmsinfo.jobs[siteid]['activated']            
           
            pending_pilots = batchinfo.queues[self.wmsqueue.apfqueue].pending            
            running_pilots = batchinfo.queues[self.wmsqueue.apfqueue].running
            #running_pilots = status.batch.get('2', 0)
            nbpilots = pending_pilots + running_pilots

            out = max(0, nbjobs - pending_pilots)
            # check if the config file has attribute MAX_JOBS_TORUN
            if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'sched.activated.max_jobs_torun'):
                MAX_JOBS_TORUN = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'sched.activated.max_jobs_torun')
                self.log.debug('calcSubmitNum: there is a MAX_JOBS_TORUN number setup to %s' %MAX_JOBS_TORUN)
                out_2 = max(0, MAX_JOBS_TORUN - nbpilots) # this is to prevent having a negative number as solution
                out = min(out, out_2)

        # check if the config file has attribute MAX_PILOTS_PER_CYCLE 
        if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'sched.activated.max_pilots_per_cycle'):
            MAX_PILOTS_PER_CYCLE = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'sched.activated.max_pilots_per_cycle')
            self.log.debug('calcSubmitNum: there is a MAX_PILOTS_PER_CYCLE number setup to %s' %MAX_PILOTS_PER_CYCLE)
            out = min(out, MAX_PILOTS_PER_CYCLE)

        # check if the config file has attribute MIN_PILOTS_PER_CYCLE 
        if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'sched.activated.min_pilots_per_cycle'):
            MIN_PILOTS_PER_CYCLE = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'sched.activated.min_pilots_per_cycle')
            self.log.debug('calcSubmitNum: there is a MIN_PILOTS_PER_CYCLE number setup to %s' %MIN_PILOTS_PER_CYCLE)
            out = max(out, MIN_PILOTS_PER_CYCLE)

        # check if the config file has attribute MAX_PILOTS_PENDING
        if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'sched.activated.max_pilots_pending'):
            MAX_PILOTS_PENDING = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'sched.activated.max_pilots_pending')
            self.log.debug('calcSubmitNum: there is a MIN_PILOTS_PER_CYCLE number setup to %s' %MAX_PILOTS_PENDING)
            out2 = max(0, MAX_PILOTS_PENDING - pending_pilots) # this is to prevent having a negative number as solution
            out = min(out, out2)

        self.log.debug('calcSubmitNum (activated_jobs=%s; pending_pilots=%s; running_pilots=%s;) : Leaving returning %s' %(activated_jobs, 
                                                                                                                           pending_pilots, 
                                                                                                                           running_pilots, 
                                                                                                                           out))
        return out
