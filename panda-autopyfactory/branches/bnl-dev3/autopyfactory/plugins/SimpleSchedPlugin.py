#! /usr/bin/env python
#
#
#


from autopyfactory.factory import SchedInterface
import logging

class SchedPlugin(SchedInterface):
    
    
    def __init__(self):
        self.log = logging.getLogger("main.simpleschedplugin")
        self.log.debug("SimpleSchedPlugin initializing...")
