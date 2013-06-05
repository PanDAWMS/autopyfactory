#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class ScaleSchedPlugin(SchedInterface):
    id = 'scale'
    
    '''   
    applies an scale factor to the previous value of nsub.
    Returns the input as it is in case there is no scale factor defined.
    '''   
 
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)
            self.factor = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.scale.factor', 'getfloat', default_value=1.0, logger=self.log)
            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):

        self.log.debug('calcSubmitNum: Starting with n=%s' %n)
        out = int(n * self.factor)
        self.log.info('calcSubmitNum: return with out=%s' %out)
        msg = "Scale=%s,factor=%s,ret=%s" %(n, self.factor, out )
        return (out, msg) 
