#! /usr/bin/env python
#

from autopyfactory.factory import SchedInterface
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

        self._valid = True
        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            # testmode vars
            self.testmode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.statustest.allowed', 'getboolean', logger=self.log)
            self.pilots_in_test_mode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.statustest.pilots', 'getint', default_value=0, logger=self.log)

            self.log.info("SchedPlugin: Object initialized.")
        except:
            self._valid = False

    def valid(self):
        return self._valid

    def calcSubmitNum(self, nsub=0):
        
        self.log.debug('calcSubmitNum: Starting.')

        self.wmsinfo = self.apfqueue.wmsstatus_plugin.getInfo(maxtime = self.apfqueue.wmsstatusmaxtime)
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        if self.wmsinfo is None:
            self.log.warning("wsinfo is None!")
            out = self.default
        elif self.batchinfo is None:
            self.log.warning("self.batchinfo is None!")
            out = self.default            
        elif not self.wmsinfo.valid() and self.batchinfo.valid():
            out = self.default
            self.log.warn('calcSubmitNum: a status is not valid, returning default = %s' %out)
        else:
            # Carefully get wmsinfo, activated. 
            self.siteid = self.apfqueue.siteid
            self.log.debug("Siteid is %s" % self.siteid)

            siteinfo = self.wmsinfo.site
            sitestatus = siteinfo[self.siteid].status
            self.log.debug('calcSubmitNum: site status is %s' %sitestatus)

            if sitestatus == 'test':
                if self.testmode:
                    self.log.info('_calc: testmode is enabled, returning default %s' %self.pilots_in_test_mode)
                    out self.pilots_in_test_mode
                else:
                    out = nsub 
        return out


