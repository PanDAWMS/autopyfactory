#! /usr/bin/env python
#


from autopyfactory.factory import SchedInterface
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

    #__metaclass__ = Singleton
    # makes sense for this class to be a Singleton???

    id = 'maxperfactory'
    
    def __init__(self, apfqueue):

        self._valid = True
        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            self.max_pilots_per_factory = self.apfqueue.fcl.generic_get('Factory', 'max_pilots_per_factory', 'getint', logger=self.log)

            # testmode vars
            #self.testmode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.testmode.allowed', 'getboolean', logger=self.log)
            #self.pilots_in_test_mode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.activated.testmode.pilots', 'getint', default_value=0, logger=self.log)


            self.log.info("SchedPlugin: Object initialized.")
        except:
            self._valid = False

    def valid(self):
        return self._valid

    def calcSubmitNum(self, nsub=0):
        """ 
        """

        self.log.debug('calcSubmitNum: Starting.')

        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)
        self.total_pilots = 0 
        for batchqueue in self.batchinfo.keys():  # is this the best way to get the list of batch queues??
            self.total_pilots += self.batchinfo.running
        self.log.info('calcSubmitNum: the total number of current pilots being handled by the factory is %s' %self.total_pilots)

        out = min(self.total_pilots, self.max_pilots_per_factory)
        self.log.info('calcSubmitNum: return with out = %s' %out)
        return out
