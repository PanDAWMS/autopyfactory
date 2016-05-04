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

class StatusOfflineSchedPlugin(SchedInterface):
    id = 'statusoffline'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            # offlinemode vars
            self.testmode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.statusoffline.allowed', 'getboolean', logger=self.log)
            self.pilots_in_offline_mode = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.statusoffline.pilots', 'getint', default_value=0, logger=self.log)

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

            cloud = siteinfo[self.siteid].cloud
            cloudinfo = self.wmsinfo.cloud
            cloudstatus = cloudinfo[cloud].status
            self.log.debug('calcSubmitNum: cloud %s status is %s' %(cloud, cloudstatus))

            # choosing algorithm 
            if cloudstatus == 'offline' or sitestatus == 'offline':
                if self.testmode:
                    self.log.info('calcSubmitNum: offline is enabled, returning out = %s' %self.pilots_in_offline_mode)
                    nsub = self.pilots_in_offline_mode

            return nsub 
            

