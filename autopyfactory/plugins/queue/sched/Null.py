#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class Null(SchedInterface):
    """       
    Null plugin, returning always 0. 
    The purpose is to have a plugin doing nothing when 
    other features of APF have to be tested, but there is no
    interest on submitting any actual pilot at all. 
    """       
    def __init__(self, apfqueue, config, section):
        try:
            self.log = logging.getLogger('autopyfactory.sched.%s' %apfqueue.apfqname)
            self.log.debug("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        msg = "Null:in=%d,out=0" % n
        return (0, msg)
