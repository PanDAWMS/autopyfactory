#! /usr/bin/env python
#


from autopyfactory.interfaces import SchedInterface
from autopyfactory.interfaces import Singleton
import logging


class MaxPerFactorySchedPlugin(SchedInterface):

    id = 'maxperfactory'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            self.max_pilots_per_factory = self.apfqueue.fcl.generic_get('Factory', 'maxperfactory.maximum', 'getint')

            self.log.debug("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        """ 
        """

        self.log.debug('Starting.')

        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        self.total_pilots = 0 
        for batchqueue in self.batchinfo.keys():  
            self.total_pilots += self.batchinfo[batchqueue].running
            self.total_pilots += self.batchinfo[batchqueue].pending
        self.log.info('the total number of current pending+running pilots being handled by the factory is %s' %self.total_pilots)

        out = n
        msg = None

        if self.total_pilots > self.max_pilots_per_factory:
            out = 0
        elif n + self.total_pilots > self.max_pilots_per_factory:
            out = self.max_pilots_per_factory - self.total_pilots

        # Catch all to prevent negative numbers
        #if n < 0:
        #    self.log.info('calculated output was negative. Returning 0')
        #    out = 0

        self.log.info('initial n = %s total_pilots = %s max_per_factory = %s Return=%s' %(n, self.total_pilots, self.max_pilots_per_factory, out))
        msg = 'MaxPerFactory:in=%s,total=%s,maxperfactory=%s;ret=%s' %(n, self.total_pilots, self.max_pilots_per_factory, out)
        return (out, msg)
