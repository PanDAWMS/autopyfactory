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
                        out = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'defaultnbpilots')
                        self.log.info('calcSubmitNum: status is not valid, returning default = %s' %out)
                else:
                        nqueue = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'nqueue')
                        nbpilots = status.batch.get('1', 0)
                        # '1' means pilots in Idle status

                        if nqueue > nbpilots:
                                out = nqueue - nbpilots
                        else:
                                out = 0

                # reducing the number of pilot by a ratio, if so
                # This can be useful when one knows that there are several pilots queues 
                # pulling jobs from the same panda siteid (a job queue)
                if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'pilotratio'):
                        ratio = self.wmsqueue.qcl.getfloat(self.wmsqueue.apfqueue, 'pilotratio')
                        out = int(round(out*ratio))               
 
                # check if the config file has attribute maxPilotsPerCycle
                if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'maxPilotsPerCycle'):
                        maxPilotsPerCycle = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'maxPilotsPerCycle')
                        out = min(out, maxPilotsPerCycle)

                self.log.debug('calcSubmitNum: Leaving returning %s' %out)
                return out
