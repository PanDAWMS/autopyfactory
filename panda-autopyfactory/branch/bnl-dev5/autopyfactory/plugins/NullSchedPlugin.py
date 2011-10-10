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
        '''       
        Null plugin, returning always 0. 
        The purpose is to have a plugin doing nothing when 
        other features of APF have to be tested, but there is no
        interest on submitting any actual pilot at all. 
        '''       
 
        def __init__(self, wmsqueue):
                self.log = logging.getLogger("main.schedplugin[%s]" %wmsqueue.apfqueue)
                self.log.info("SchedPlugin: Object initialized.")

        def calcSubmitNum(self, status):
                return 0
