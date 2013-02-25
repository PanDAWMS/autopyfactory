#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class FillactivatedSchedPlugin(SchedInterface):
    id = 'fillactivated'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            # A default value is required. 
            self.default = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.fillactivated.default', 'getint', default_value=0, logger=self.log)    
            self.log.debug('SchedPlugin: default = %s' %self.default)
            
            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, nsub=0):
        """ 
        returns nb of Activated Jobs - nb of Pending Pilots
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
            self.log.info("Siteid is %s" % self.siteid)

        out = _calc_online() 
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
        
        self.log.info('_calc_online (activated=%s; pending=%s; running=%s;) : Return=%s' %(activated_jobs, 
                                                                                         pending_pilots, 
                                                                                         running_pilots, 
                                                                                         out))
        return out
