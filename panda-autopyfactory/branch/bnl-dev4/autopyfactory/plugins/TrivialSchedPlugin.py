#! /usr/bin/env python
#
#
#


from autopyfactory.factory import SchedInterface
import logging

class SchedPlugin(SchedInterface):
        
        def __init__(self):
                self.log = logging.getLogger("main.simpleschedplugin")
                self.log.debug("TrivialSchedPlugin initializing...")

        def calcSubmitNum(self, status):
                """ always return 0
                """
                return 0


                
