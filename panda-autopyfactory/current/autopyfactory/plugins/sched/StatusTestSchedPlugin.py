#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class StatusTestSchedPlugin(SchedInterface):
    id = 'statustest'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            self.pilots_in_test_mode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.statustest.pilots', 'getint', default_value=0)

            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        
        self.log.debug('calcSubmitNum: Starting.')

        self.wmsinfo = self.apfqueue.wmsstatus_plugin.getInfo(maxtime = self.apfqueue.wmsstatusmaxtime)
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        if self.wmsinfo is None:
            self.log.warning("wmsinfo is None!")
            #out = self.default
            out = 0
            msg = "StatusTest,no wmsinfo,ret=0"
        elif self.batchinfo is None:
            self.log.warning("self.batchinfo is None!")
            #out = self.default            
            out = 0
            msg = "StatusTest,no batchinfo,ret=0"
        elif not self.wmsinfo.valid() and self.batchinfo.valid():
            #out = self.default
            out = 0
            msg = "StatusTest,no wms/batchinfo,ret=0"
            self.log.warn('calcSubmitNum: a status is not valid, Return=%s' %out)
        else:
            # Carefully get wmsinfo, activated. 
            self.siteid = self.apfqueue.siteid
            self.log.debug("Siteid is %s" % self.siteid)

            siteinfo = self.wmsinfo.site
            sitestatus = siteinfo[self.siteid].status
            self.log.debug('calcSubmitNum: site status is %s' %sitestatus)

            out = n
            msg = None

            if sitestatus == 'test':
                self.log.info('calcSubmitNum: Return=%s' %self.pilots_in_test_mode)
                out= self.pilots_in_test_mode
                msg='StatusTest,ret=%s' %self.pilots_in_test_mode

        return (out, msg)


