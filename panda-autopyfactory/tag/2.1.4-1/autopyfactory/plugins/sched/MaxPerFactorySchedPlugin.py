#! /usr/bin/env python
#


from autopyfactory.interfaces import SchedInterface
from autopyfactory.factory import Singleton
import logging

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class MaxPerFactorySchedPlugin(SchedInterface):

    id = 'maxperfactory'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            self.max_pilots_per_factory = self.apfqueue.fcl.generic_get('Factory', 'maxperfactory.maximum', 'getint', logger=self.log)

            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, nsub=0):
        """ 
        """

        self.log.debug('calcSubmitNum: Starting.')

        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        self.total_pilots = 0 
        for batchqueue in self.batchinfo.keys():  
            self.total_pilots += self.batchinfo[batchqueue].running
            self.total_pilots += self.batchinfo[batchqueue].pending
        self.log.info('calcSubmitNum: the total number of current pending+running pilots being handled by the factory is %s' %self.total_pilots)

        if self.total_pilots > self.max_pilots_per_factory:
            nsub = 0
        elif nsub + self.total_pilots > self.max_pilots_per_factory:
            nsub = self.max_pilots_per_factory - self.total_pilots

        # Catch all to prevent negative numbers
        #if nsub < 0:
        #    self.log.info('calcSubmitNum: calculated output was negative. Returning 0')
        #    nsub = 0

        self.log.info('calcSubmitNum: initial nsub = %s total_pilots = %s max_per_factory = %s returning = %s' %(nsub, self.total_pilots, self.max_pilots_per_factory, nsub))
        return nsub 
