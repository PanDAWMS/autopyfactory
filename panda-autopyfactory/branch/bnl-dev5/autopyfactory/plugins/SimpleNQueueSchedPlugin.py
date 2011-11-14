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

        self.default = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'sched.simplenqueue.default')
        self.log.debug('SchedPlugin: there is a default number setup to %s' %self.default)

        self.nqueue = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'sched.simplenqueue.nqueue')
        self.log.debug('SchedPlugin: there is a default number setup to %s' %self.default)

        self.maxpilotspercycle = None

        if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'sched.simplenqueue.maxpilotspercycle'):
            self.maxpilotspercycle = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'sched.simplenqueue.maxpilotspercycle')
            self.log.debug('SchedPlugin: there is a maxpilotspercycle number setup to %s' %maxpilotspercycle)

        self.log.info("SchedPlugin: Object initialized.")

    def calcSubmitNum(self, status):
        """ 
        is nqueue > number of idle?
           no  -> return 0
           yes -> 
              is maxPilotsPerCycle defined?
                 yes -> return min(nqueue - nbpilots, maxPilotsPerCycle)
                 no  -> return (nqueue - nbpilots)
        """

        self.log.debug('calcSubmitNum: Starting with input %s' %status)

        if not status:
            out = 0
        elif not status.valid():
            out = self.default
            self.log.info('calcSubmitNum: status is not valid, returning default = %s' %out)
        else:
            nbpilots = status.batch.get('1', 0)
            # '1' means pilots in Idle status

            if self.nqueue > nbpilots:
                    out = self.nqueue - nbpilots
            else:
                    out = 0

        # check if the config file has attribute maxpilotspercycle
        if self.maxpilotspercycle:
            out = min(out, self.maxpilotspercycle)

        self.log.debug('calcSubmitNum: Leaving returning %s' %out)
        return out
