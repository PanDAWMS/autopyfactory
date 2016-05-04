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

class WeightedActivatedSchedPlugin(SchedInterface):
    id = 'weightedactivated'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            # --- weights ---
            self.activated_w = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.weightedactivated.activated', 'getfloat', default_value=1.0, logger=self.log)
            self.pending_w = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.weightedactivated.pending', 'getfloat', default_value=1.0, logger=self.log)
            self.log.debug("SchedPlugin: weight values are activated_w=%s, pending_w=%s." %(self.activated_w, self.pending_w))

            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, nsub=0):
        """ 
        It returns nb of Activated Jobs - nb of Pending Pilots
        But before making that calculation, it applies a scaling factor
        to both values: activated and pending
        """

        self.log.debug('calcSubmitNum: Starting.')

        self.wmsinfo = self.apfqueue.wmsstatus_plugin.getInfo(maxtime = self.apfqueue.wmsstatusmaxtime)
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        if self.wmsinfo is None:
            self.log.warning("calcSubmitNum: wsinfo is None!")
            out = self.default
        elif self.batchinfo is None:
            self.log.warning("calcSubmitNum: self.batchinfo is None!")
            out = self.default            
        elif not self.wmsinfo.valid() and self.batchinfo.valid():
            out = self.default
            self.log.warn('calcSubmitNum: a status is not valid, returning default = %s' %out)
        else:
            # Carefully get wmsinfo, activated. 
            self.siteid = self.apfqueue.siteid
            self.log.info("calcSubmitNum: siteid is %s" % self.siteid)

            out = self._calc()
        return out

    def _calc(self):
        
        # initial default values. 
        activated_jobs = 0
        pending_pilots = 0

        jobsinfo = self.wmsinfo.jobs
        self.log.debug("_calc: jobsinfo class is %s" % jobsinfo.__class__ )

        try:
            sitedict = jobsinfo[self.siteid]
            self.log.debug("_calc: sitedict class is %s" % sitedict.__class__ )
            activated_jobs = sitedict.ready
        except KeyError:
            # This is OK--it just means no jobs in any state at the siteid. 
            self.log.error("_calc: siteid %s not present in jobs info from WMS" % self.siteid)
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
        activated_jobs = int(activated_jobs * self.activated_w)
        pending_pilots = int(pending_pilots * self.pending_w)

        out = max(0, activated_jobs - pending_pilots)

        self.log.info('_calc (activated=%s; pending=%s) : Return=%s' %(activated_jobs, 
                                                                       pending_pilots, 
                                                                       out))
        return out
