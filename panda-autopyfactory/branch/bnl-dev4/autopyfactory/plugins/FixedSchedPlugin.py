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
                returns always a fixed number of pilots
                """

                if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'fixedPilotsPerCycle'):
                        out = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'fixedPilotsPerCycle')
                        self.log.debug('calcSubmitNum: there is a fixedPilotsPerCycle number setup to %s' %out)
                else:
                        self.log.debug('calcSubmitNum: there is not a fixedPilotsPerCycle, returning 0')
                        out = 0

                return out

