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

            self.log.debug("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        
        self.log.debug('Starting.')
        self.wmsqueueinfo = self.apfqueue.wmsstatus_plugin.getInfo(queue=self.apfqueue.wmsqueue, maxtime = self.apfqueue.wmsstatusmaxtime)
        self.siteinfo = self.apfqueue.wmsstatus_plugin.getSiteInfo(site=self.apfqueue.wmsqueue, maxtime = self.apfqueue.wmsstatusmaxtime)
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(queue=self.apfqueue.apfqname, maxtime = self.apfqueue.batchstatusmaxtime)

        if self.wmsqueueinfo is None or self.batchinfo is None or self.siteinfo is None:
            self.log.warning("wmsinfo, batchinfo, or siteinfo is None!")
            out = 0
            msg = "StatusTest:no wms/batch/siteinfo,ret=0"
        else:
            sitestatus = self.siteinfo.status
            self.log.debug('site status is %s' %sitestatus)
            out = n
            msg = None
            if sitestatus == 'test':
                self.log.info('Return=%s' %self.pilots_in_test_mode)
                out= self.pilots_in_test_mode
                msg='StatusTest,ret=%s' %self.pilots_in_test_mode

        return (out, msg)

