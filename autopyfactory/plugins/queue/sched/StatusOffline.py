#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import logging


class StatusOffline(SchedInterface):
    id = 'statusoffline'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" % apfqueue.apfqname)

            self.pilots_in_offline_mode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 
                                                                        'sched.statusoffline.pilots', 
                                                                        'getint', 
                                                                        default_value=0)

            self.log.trace("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        
        self.log.trace('Starting.')
        self.wmsqueueinfo = self.apfqueue.wmsstatus_plugin.getInfo(queue=self.apfqueue.wmsqueue, 
                                                                   maxtime = self.apfqueue.wmsstatusmaxtime)
        
        self.siteinfo = self.apfqueue.wmsstatus_plugin.getSiteInfo(site=self.apfqueue.wmsqueue,
                                                                    maxtime = self.apfqueue.wmsstatusmaxtime)

        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(queue=self.apfqueue.apfqname, 
                                                                  maxtime = self.apfqueue.batchstatusmaxtime)

        #if self.siteinfo:
            #sitecloud = self.siteinfo.cloud
            #self.cloudinfo = self.apfqueue.wmsstatus_plugin.getCloudInfo(cloud=sitecloud, 
            #                                                      maxtime = self.apfqueue.batchstatusmaxtime)

        #if self.wmsqueueinfo is None or self.batchinfo is None or self.cloudinfo is None:
        if self.wmsqueueinfo is None or self.batchinfo is None:
            self.log.warning("wmsinfo, batchinfo, or cloudinfo is None!")
            out = 0
            msg = "StatusOffline[no wms/batch/cloudinfo]:in=%s;ret=0" %n
        else:
            sitestatus = self.siteinfo.status
            self.log.trace('site status is %s' %sitestatus)

            #cloudstatus = self.cloudinfo.status
            #self.log.trace('cloud %s status is %s' %(sitecloud, cloudstatus))

            out = n
            msg = None

            # choosing algorithm 
            #if cloudstatus == 'offline' or sitestatus == 'offline':
            if sitestatus == 'offline':
                self.log.info('Return=%s' %self.pilots_in_offline_mode)
                out = self.pilots_in_offline_mode
                msg = "StatusOffline:(offline);in=%s;ret=%s" %(n, self.pilots_in_offline_mode)
            else:
                msg = "StatusOffline:(not offline);in=%s;ret=%s" %(n, self.pilots_in_offline_mode)
        self.log.info(msg)
        return (out, msg) 
            

