#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class StatusTestSchedPlugin(SchedInterface):
    id = 'statustest'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            # testmode vars
            self.testmode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.statustest.allowed', 'getboolean', logger=self.log)
            self.pilots_in_test_mode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.statustest.pilots', 'getint', default_value=0, logger=self.log)

            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, nsub=0):
        
        self.log.debug('calcSubmitNum: Starting.')

        self.wmsinfo = self.apfqueue.wmsstatus_plugin.getInfo(maxtime = self.apfqueue.wmsstatusmaxtime)
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        if self.wmsinfo is None:
            self.log.warning("wsinfo is None!")
            #out = self.default
            out = 0
        elif self.batchinfo is None:
            self.log.warning("self.batchinfo is None!")
            #out = self.default            
            out = 0
        elif not self.wmsinfo.valid() and self.batchinfo.valid():
            #out = self.default
            out = 0
            self.log.warn('calcSubmitNum: a status is not valid, returning default = %s' %out)
        else:
            # Carefully get wmsinfo, activated. 
            self.siteid = self.apfqueue.siteid
            self.log.debug("Siteid is %s" % self.siteid)

            siteinfo = self.wmsinfo.site
            sitestatus = siteinfo[self.siteid].status
            self.log.debug('calcSubmitNum: site status is %s' %sitestatus)

            out = nsub

            if sitestatus == 'test':
                if self.testmode:
                    self.log.info('calcSubmitNum: testmode is enabled, returning out = %s' %self.pilots_in_test_mode)
                    out = self.pilots_in_test_mode
        return out 


