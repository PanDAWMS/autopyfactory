#! /usr/bin/env python
#
import logging

from autopyfactory.interfaces import SchedInterface

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class KeepNRunningSchedPlugin(SchedInterface):
    '''
    This plugin strives to keep a certain number of jobs/pilots/VMs running, regardless 
    of ready/activated or input. 
    
    Understands Retiring VM job state. 
    
    May output a negative number, if keep_running is less than current running. 
      
           
    '''
    id = 'keepnrunning'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" % apfqueue.apfqname)
            self.keep_running = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.keepnrunning.keep_running', 'getint', logger=self.log)
            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, nsub=0):
        """ 
        It just returns nb of Activated Jobs - nb of Pending Pilots
        """
        input = nsub
        self.log.debug('calcSubmitNum: Starting.')

        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        if self.batchinfo is None:
            self.log.warning("self.batchinfo is None!")
            out = 0
        elif not self.batchinfo.valid():
            out = 0 
            self.log.warn('calcSubmitNum: a status is not valid, returning default = %s' %out)
        else:
            self.key = self.apfqueue.wmsqueue
            self.log.info("Key is %s" % self.key)

            out = self._calc(input)
        return out

    def _calc(self, input):
        '''
        algorithm 
        '''
        
        # initial default values. 
        activated_jobs = 0
        pending_pilots = 0
        running_pilots = 0

        jobsinfo = self.wmsinfo.jobs
        self.log.debug("jobsinfo class is %s" % jobsinfo.__class__ )

        try:
            sitedict = jobsinfo[self.key]
            self.log.debug("sitedict class is %s" % sitedict.__class__ )
            activated_jobs = sitedict.ready
        except KeyError:
            # This is OK--it just means no jobs in any state at the key. 
            self.log.error("key: %s not present in jobs info from WMS" % self.key)

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

        out = max(0, activated_jobs - pending_pilots)
        self.log.info('_calc() (input=%s; activated=%s; pending=%s; running=%s;) : Return=%s' %(input,
                                                                                         activated_jobs, 
                                                                                         pending_pilots, 
                                                                                         running_pilots, 
                                                                                         out))
        return out




