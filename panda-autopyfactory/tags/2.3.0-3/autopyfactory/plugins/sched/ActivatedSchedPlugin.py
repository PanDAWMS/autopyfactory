#! /usr/bin/env python
#

import logging

from autopyfactory.interfaces import SchedInterface


class ActivatedSchedPlugin(SchedInterface):
    id = 'activated'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("sched.activated[%s]" %apfqueue.apfqname)

            self.max_jobs_torun = None
            self.max_pilots_per_cycle = None
            self.min_pilots_per_cycle = None
            self.max_pilots_pending = None
            
            # A default value is required. 
            self.default = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.default', 'getint', default_value=0)    
            self.log.debug('SchedPlugin: default = %s' %self.default)
            
            self.max_jobs_torun = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.max_jobs_torun', 'getint')
            self.max_pilots_per_cycle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.max_pilots_per_cycle', 'getint')
            self.min_pilots_per_cycle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.min_pilots_per_cycle', 'getint')
            self.min_pilots_pending = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.min_pilots_pending', 'getint')
            self.max_pilots_pending = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.max_pilots_pending', 'getint')
            # testmode vars
            self.testmode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.testmode.allowed', 'getboolean')
            self.pilots_in_test_mode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.testmode.pilots', 'getint', default_value=0)
            self.max_pending_in_test_mode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.testmode.max_pending', 'getint', default_value=10)

            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, nsub=0):
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
        self.log.debug('Starting.')

        self.wmsinfo = self.apfqueue.wmsstatus_plugin.getInfo(maxtime = self.apfqueue.wmsstatusmaxtime)
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        if self.wmsinfo is None:
            self.log.warning("wsinfo is None!")
            out = self.default
        elif self.batchinfo is None:
            self.log.warning("self.batchinfo is None!")
            out = self.default            
        elif not self.wmsinfo.valid() and self.batchinfo.valid():
            out = self.default
            self.log.warn('a status is not valid, returning default = %s' %out)
        else:
            # Carefully get wmsinfo, activated. 
            self.wmsqueue = self.apfqueue.wmsqueue
            self.log.info("Siteid is %s" % self.wmsqueue)

            siteinfo = self.wmsinfo.site
            sitestatus = siteinfo[self.wmsqueue].status
            self.log.info('site status is %s' %sitestatus)

            cloud = siteinfo[self.wmsqueue].cloud
            cloudinfo = self.wmsinfo.cloud
            cloudstatus = cloudinfo[cloud].status
            self.log.info('cloud %s status is %s' %(cloud, cloudstatus))

            # choosing algorithm 
            if cloudstatus == 'offline':
                return self._calc_offline()

            #if sitestatus == 'online':
            #    out = self._calc_online()
            if sitestatus == 'test':
                out = self._calc_test()
            elif sitestatus == 'offline':
                out = self._calc_offline()
            else:
                # default
                out = self._calc_online()
        return out

    def _calc_online(self):
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
        
        if self.max_jobs_torun: 
            out = min(out, self.max_jobs_torun - all_pilots)

        if self.min_pilots_pending:
            out = max(out, self.min_pilots_pending - pending_pilots)
        
        if self.max_pilots_per_cycle:
            out = min(out, self.max_pilots_per_cycle)

        if self.min_pilots_per_cycle:
            out = max(out, self.min_pilots_per_cycle)

        if self.max_pilots_pending:
            out = min(out, self.max_pilots_pending - pending_pilots)
       
        self.log.info('_calc_online (activated=%s; pending=%s; running=%s;) : Return=%s' %(activated_jobs, 
                                                                                         pending_pilots, 
                                                                                         running_pilots, 
                                                                                         out))
        return out

    def _calc_test(self):
        '''
        algorithm when wmssite is in test mode
        '''
        if not self.testmode:
            self.log.info('testmode is not enabled. Calling the normal online algorithm')
            return self._calc_online()

        pending_pilots = 0

        try:
            pending_pilots = self.batchinfo[self.apfqueue.apfqname].pending  # using the new info objects
        except KeyError:
            # This is OK--it just means no jobs. 
            pass

        if pending_pilots > self.max_pending_in_test_mode:
            out = 0
            self.log.info('(pending=%s > max_pending=%s;) : Return=%s' %(pending_pilots,
                                                                                   self.max_pending_in_test_mode,
                                                                                   out))
        else:
            out = self.pilots_in_test_mode
            self.log.info('(pending=%s; max_pending=%s;) : Return=%s' %(pending_pilots,
                                                                                  self.max_pending_in_test_mode,
                                                                                  out))

        return out

    def _calc_offline(self):
        '''
        algorithm when wmssite is in offline mode
        '''
        # default, just return 0
        return 0 

