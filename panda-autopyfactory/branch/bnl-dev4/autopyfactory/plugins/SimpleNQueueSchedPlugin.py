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
                self.log = logging.getLogger("main.schedplugin[%s]" %wmsqueue.siteid)
                self.log.info("SchedPlugin: Object initialized.")

        def calcSubmitNum(self, status):
                """ 
                is nqueue > number of idle?
                        yes -> return nqueue - nbpilots
                        no -> return 0

                This version makes use of config variable maxPilotsPerCycle.
                """

                self.log.debug('calcSubmitNum: Starting with input %s' %status)

                if not status:
                        out = 0
                elif not status.valid():
                        out = self.wmsqueue.qcl.getint(self.wmsqueue.siteid, 'defaultnbpilots')
                        self.log.info('calcSubmitNum: status is not valid, returning default = %s' %out)
                else:
                        nqueue = self.wmsqueue.qcl.getint(self.wmsqueue.siteid, 'nqueue')
                        nbpilots = status.batch.get('1', 0)
                        # '1' means pilots in Idle status

                        if nqueue > nbpilots:
                                out = nqueue - nbpilots
                        else:
                                out = 0
                
                # check if the config file has attribute maxPilotsPerCycle
                if self.wmsqueue.qcl.has_option(self.wmsqueue.siteid, 'maxPilotsPerCycle'):
                        maxPilotsPerCycle = self.wmsqueue.qcl.getint(self.wmsqueue.siteid, 'maxPilotsPerCycle')
                        out = min(out, maxPilotsPerCycle)

                self.log.debug('calcSubmitNum: Leaving returning %s' %out)
                return out
