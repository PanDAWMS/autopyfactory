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
                is number of actived jobs == 0 ?
                        yes -> return 0
                        no ->
                                is number of activated > number of idle+running pilots?
                                        yes -> return nbjobs - nbpilots
                                        no -> return 0
                """

                self.log.debug('calcSubmitNum: Starting')


                # giving an initial value to some variables
                # to prevent the logging from crashing
                nbjobs = 0
                pending_pilots = 0
                running_pilots = 0

                wmsinfo = self.wmsqueue.wmsstatus.getInfo(maxtime = self.wmsqueue.wmsstatusmaxtime)
                batchinfo = self.wmsqueue.batchstatus.getInfo(maxtime = self.wmsqueue.batchstatusmaxtime)
                
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
                    siteid = self.wmsqueue.siteid
                    self.log.debug("Siteid is %s" % siteid)
                    jobsinfo = wmsinfo.jobs
                    self.log.debug("jobsinfo class is %s" % jobsinfo.__class__ )
                    try:
                        sitedict = jobsinfo[siteid]
                        self.log.debug("sitedict class is %s" % sitedict.__class__ )
                        activated_jobs = sitedict['activated']
                    except KeyError:
                        # This is OK--it just means no jobs in any state at the siteid. 
                        self.log.error("siteid: %s not present in jobs info from WMS" % siteid)
                        activated_jobs = 0
                
                    try:
                        pending_pilots = batchinfo.queues[self.wmsqueue.apfqueue].pending
                    except KeyError:
                                        # This is OK--it just means no jobs. 
                        pass
                
                    try:
                        running_pilots = batchinfo.queues[self.wmsqueue.apfqueue].running
                    except KeyError:
                        # This is OK--it just means no jobs. 
                        pass
                

                all_pilots = pending_pilots + running_pilots

                # note: the following if-else algorithm can be written
                #       in a simpler way, but in this way is easier to 
                #       read and to understand what it does and why.
                if activated_jobs == 0:
                        out = 0
                else:
                        if activated_jobs > all_pilots:
                                out = activated_jobs - all_pilots 
                        else:
                                out = 0

                self.log.debug('calcSubmitNum (activated_jobs=%s; pending_pilots=%s; running_pilots=%s): Leaving returning %s' %(activated_jobs, pending_pilots, running_pilots, out))
                return out
