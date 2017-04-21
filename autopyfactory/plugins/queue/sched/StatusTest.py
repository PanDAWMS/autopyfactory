#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class StatusTest(SchedInterface):
    id = 'statustest'
    
    def __init__(self, apfqueue, config, section):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger()

            self.pilots_in_test_mode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.statustest.pilots', 'getint', default_value=0)

            self.log.debug("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        
        self.log.debug('Starting.')
        self.wmsqueueinfo = self.apfqueue.wmsstatus_plugin.getInfo(
                                queue=self.apfqueue.wmsqueue)
        self.siteinfo = self.apfqueue.wmsstatus_plugin.getSiteInfo(
                                site=self.apfqueue.wmsqueue)
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(
                                queue=self.apfqueue.apfqname)

        if self.wmsqueueinfo is None or self.batchinfo is None or self.siteinfo is None:
            self.log.warning("wmsinfo, batchinfo, or siteinfo is None!")
            out = 0
            msg = "StatusTest:comment=no wms/batch/siteinfo,in=%s,ret=0" %n
        else:
            sitestatus = self.siteinfo.status
            self.log.debug('site status is %s' %sitestatus)
            out = n
            if sitestatus == 'test':
                out = self.pilots_in_test_mode
                msg='StatusTest:comment=test,in=%d,out=%d' % ( n,  self.pilots_in_test_mode )
            else:
                msg='StatusTest:comment=not test,in=%s,ret=%s' % (n, out)
        return (out, msg)

