#! /usr/bin/env python
#

from autopyfactory.factory import SchedInterface
import logging

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class ActivatedSchedPlugin(SchedInterface):
    id = 'activated'
    
    def __init__(self, apfqueue):

        self._valid = True
        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            self.max_jobs_torun = None
            self.max_pilots_per_cycle = None
            self.min_pilots_per_cycle = None
            self.max_pilots_pending = None
            
            # A default value is required. 
            self.default = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.default', 'getint', default_value=0, logger=self.log)    
            self.log.debug('SchedPlugin: default = %s' %self.default)
            
            self.max_jobs_torun = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.max_jobs_torun', 'getint', logger=self.log)
            self.max_pilots_per_cycle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.max_pilots_per_cycle', 'getint', logger=self.log)
            self.min_pilots_per_cycle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.min_pilots_per_cycle', 'getint', logger=self.log)
            self.min_pilots_pending = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.min_pilots_pending', 'getint', logger=self.log)
            self.max_pilots_pending = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.max_pilots_pending', 'getint', logger=self.log)
            # testmode vars
            self.testmode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.testmode.allowed', 'getboolean', logger=self.log)
            self.pilots_in_test_mode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.testmode.pilots', 'getint', default_value=0, logger=self.log)

            self.log.info("SchedPlugin: Object initialized.")
        except:
            self._valid = False

    def valid(self):
        return self._valid

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
        self.log.debug('calcSubmitNum: Starting.')

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
            self.log.warn('calcSubmitNum: a status is not valid, returning default = %s' %out)
        else:
            # Carefully get wmsinfo, activated. 
            self.siteid = self.apfqueue.siteid
            self.log.debug("Siteid is %s" % self.siteid)

            siteinfo = self.wmsinfo.site
            sitestatus = siteinfo[self.siteid].status
            self.log.debug('calcSubmitNum: site status is %s' %sitestatus)

            cloud = siteinfo[self.siteid].cloud
            cloudinfo = self.wmsinfo.cloud
            cloudstatus = cloudinfo[cloud].status
            self.log.debug('calcSubmitNum: cloud %s status is %s' %(cloud, cloudstatus))

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
            sitedict = jobsinfo[self.siteid]
            self.log.debug("sitedict class is %s" % sitedict.__class__ )
            #activated_jobs = sitedict['activated']
            activated_jobs = sitedict.ready
        except KeyError:
            # This is OK--it just means no jobs in any state at the siteid. 
            self.log.error("siteid: %s not present in jobs info from WMS" % self.siteid)
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

        # Catch all to prevent negative numbers
        if out < 0:
            self.log.info('_calc_online: calculated output was negative. Returning 0')
            out = 0
        
        self.log.info('_calc_online (activated=%s; pending=%s; running=%s;) : Return=%s' %(activated_jobs, 
                                                                                         pending_pilots, 
                                                                                         running_pilots, 
                                                                                         out))
        return out

    def _calc_test(self):
        '''
        algorithm when wmssite is in test mode
        '''

        if self.testmode:
            self.log.info('_calc_test: testmode is enabled, returning default %s' %self.pilots_in_test_mode)
            return self.pilots_in_test_mode
        else:
            self.log.info('_calc_test: testmode is not enabled. Calling the normal online algorithm')
            return self._calc_online()
            
    def _calc_offline(self):
        '''
        algorithm when wmssite is in offline mode
        '''
        # default, just return 0
        return 0 

