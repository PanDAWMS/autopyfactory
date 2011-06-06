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

        def calcSubmitNum(self, *args):
                """because it is a trivial version, we can 
                substitute the list of input options, which would look like
                        def calcSubmitNum(self, config, activated, failed, running, transferring):
                by a simpler *args. 
                We are not going to use it here anyway, 
                but we put something for compatibility.
                """
                return 0


                
