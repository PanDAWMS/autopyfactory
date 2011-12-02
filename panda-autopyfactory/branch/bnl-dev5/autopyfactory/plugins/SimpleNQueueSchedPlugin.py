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
    
    def __init__(self, apfqueue):
        self.apfqueue = apfqueue
        self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

        self.default = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.simplenqueue.default')
        self.log.debug('SchedPlugin: there is a default number setup to %s' %self.default)

        self.nqueue = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.simplenqueue.nqueue')
        self.log.debug('SchedPlugin: there is a default number setup to %s' %self.default)

        self.maxpilotspercycle = None

        if self.apfqueue.qcl.has_option(self.apfqueue.apfqname, 'sched.simplenqueue.maxpilotspercycle'):
            self.maxpilotspercycle = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.simplenqueue.maxpilotspercycle')
            self.log.debug('SchedPlugin: there is a maxpilotspercycle number setup to %s' %maxpilotspercycle)

        self.log.info("SchedPlugin: Object initialized.")

    def calcSubmitNum(self):
        """ 
        is nqueue > number of idle?
           no  -> return 0
           yes -> 
              is maxPilotsPerCycle defined?
                 yes -> return min(nqueue - nbpilots, maxPilotsPerCycle)
                 no  -> return (nqueue - nbpilots)
        """

        self.log.debug('calcSubmitNum: Starting ')



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
            try:
                pending_pilots = batchinfo.queues[self.apfqueue.apfqname].pending
            except KeyError:
                                # This is OK--it just means no jobs. 
                pass

            if self.nqueue > pending_pilots:
                    out = self.nqueue - pending_pilots 
            else:
                    out = 0

            # check if the config file has attribute maxpilotspercycle
            if self.maxpilotspercycle:
                out = min(out, self.maxpilotspercycle)

        self.log.debug('calcSubmitNum: Leaving returning %s' %out)
        return out
