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

class ReadySchedPlugin(SchedInterface):
    id = 'ready'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" % apfqueue.apfqname)
            try:
                self.offset = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.ready.offset', 'getint', default_value=0)
            except:
                pass 
                # Not mandatory
                
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

        self.wmsinfo = self.apfqueue.wmsstatus_plugin.getInfo(maxtime = self.apfqueue.wmsstatusmaxtime)
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        if self.wmsinfo is None:
            self.log.warning("wsinfo is None!")
            out = 0 
        elif self.batchinfo is None:
            self.log.warning("self.batchinfo is None!")
            out = 0
        elif not self.wmsinfo.valid() and self.batchinfo.valid():
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

        out = max(0, ( activated_jobs - self.offset)  - pending_pilots )
        self.log.info('_calc() (input=%s; activated=%s; offset=%s pending=%s; running=%s;) : Return=%s' %(input,
                                                                                         activated_jobs,
                                                                                         self.offset, 
                                                                                         pending_pilots, 
                                                                                         running_pilots, 
                                                                                         out))
        return out
