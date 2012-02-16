#! /usr/bin/env python
#

from autopyfactory.factory import SchedInterface
import logging

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class SimpleNQueueSchedPlugin(SchedInterface):
    
    def __init__(self, apfqueue):
        self.id = 'simplenqueue'
        self._valid = True
        try:
            self.apfqueue = apfqueue
            self.siteid = self.apfqueue.siteid
            self.log = logging.getLogger("main.schedplugin[%s]" %apfqueue.apfqname)

            self.default = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.simplenqueue.default')
            self.log.debug('SchedPlugin: there is a default number setup to %s' %self.default)

            self.nqueue = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.simplenqueue.nqueue')
            self.log.debug('SchedPlugin: there is a nqueue number setup to %s' %self.nqueue)

            self.cloud = self.apfqueue.qcl.get(self.apfqueue.apfqname, 'cloud')
            self.log.debug('SchedPlugin: cloud is setup to %s' %self.cloud)

            self.status = None
            if self.apfqueue.qcl.has_option(self.apfqueue.apfqname, 'status'):
                    self.status = self.apfqueue.qcl.get(self.apfqueue.apfqname, 'status')
                    self.log.debug('SchedPlugin: there is a status value setup to %s' %self.status)

            self.depthboost = None
            if self.apfqueue.qcl.has_option(self.apfqueue.apfqname, 'sched.simplenqueue.depthboost'):
                    self.depthboost = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.simplenqueue.depthboost')
                    self.log.debug('SchedPlugin: there is a depthboost number setup to %s' %self.depthboost)

            self.pilotlimit = None
            if self.apfqueue.qcl.has_option(self.apfqueue.apfqname, 'sched.simplenqueue.pilotlimit'):
                    self.pilotlimit = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.simplenqueue.pilotlimit')
                    self.log.debug('SchedPlugin: there is a pilotlimit number setup to %s' %self.pilotlimit)

            self.transferringlimit = None
            if self.apfqueue.qcl.has_option(self.apfqueue.apfqname, 'sched.simplenqueue.transferringlimit'):
                    self.transferringlimit = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.simplenqueue.transferringlimit')
                    self.log.debug('SchedPlugin: there is a transferringlimit number setup to %s' %self.transferringlimit)

            self.maxpilotspercycle = None
            if self.apfqueue.qcl.has_option(self.apfqueue.apfqname, 'sched.simplenqueue.maxpilotspercycle'):
                self.maxpilotspercycle = self.apfqueue.qcl.getint(self.apfqueue.apfqname, 'sched.simplenqueue.maxpilotspercycle')
                self.log.debug('SchedPlugin: there is a maxpilotspercycle number setup to %s' %self.maxpilotspercycle)

            self.log.info("SchedPlugin: Object initialized.")
        except:
            self._valid = False

    def valid(self):
        return self._valid

    def calcSubmitNum(self):
        """ 
        is nqueue > number of idle?
           no  -> return 0
           yes -> 
              is maxPilotsPerCycle defined?
                 yes -> return min(nqueue - nbpilots, maxPilotsPerCycle)
                 no  -> return (nqueue - nbpilots)
        """

        self.log.debug('calcSubmitNum: Starting ')

        wmsinfo = self.apfqueue.wmsstatus.getInfo(maxtime = self.apfqueue.wmsstatusmaxtime)
        batchinfo = self.apfqueue.batchstatus.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)

        if wmsinfo is None:
            self.log.warning("wsinfo is None!")
            out = self.default
        elif batchinfo is None:
            self.log.warning("batchinfo is None!")
            out = self.default
        elif not wmsinfo.valid() and batchinfo.valid():
            out = self.default
            self.log.warn('calcSubmitNum: a status is not valid, returning default = %s' %out)
        else:
            jobsinfo = wmsinfo.jobs
            sitedict = jobsinfo[self.siteid]                
            cloudinfo = wmsinfo.cloud
            siteinfo = wmsinfo.site
            
            if cloudinfo[self.cloud].status == 'offline':
                self.log.info('calcSubmitNum: cloud %s is offline - will not submit pilots' %self.cloud)
            if siteinfo[self.siteid].status == 'offline':
                self.log.info('calcSubmitNum: site %s is offline - will not submit pilots' %self.siteid)
            if siteinfo[self.siteid].status == 'error':
                self.log.info('calcSubmitNum: site %s is in an error state - will not submit pilots' %self.siteid)
            if cloudinfo[self.cloud].status == 'test':
                self.log.info('calcSubmitNum: cloud %s is in test mode' %self.cloud)
                cloudTestStatus = True
            else:
                cloudTestStatus = False
                
            # Now normal queue submission algorithm begins
            #allpilots = batchinfo[self.apfqueue.apfqname].pending +\
            #            batchinfo[self.apfqueue.apfqname].running +\
            #            batchinfo[self.apfqueue.apfqname].done +\
            #            batchinfo[self.apfqueue.apfqname].error +\
            #            batchinfo[self.apfqueue.apfqname].unknown +\
            #            batchinfo[self.apfqueue.apfqname].suspended
            allpilots = batchinfo[self.apfqueue.apfqname].total
            inactivepilots = allpilots - batchinfo[self.apfqueue.apfqname].running
            ready = sitedict.ready

            if self.pilotlimit and allpilots > self.pilotlimit:
                self.log.info('calcSubmitNum: will not submit more pilots for apfqueue %s' %self.apfqueue.apfqname)       

            # FIXME: how to deal with transferring?

            if self.status == 'test' or cloudTestStatus:
                if inactivepilots > 0 and allpilots > self.nqueue:
                        self.log.info('calcSubmitNum: test apfqueue %s has %s pilots, %s queued. Doing nothing' %(self.apfqueue.apfqname, allpilots, inactivepilots))
                else:
                        out = 1
                        self.log.info('calcSubmitNum: test apfqueue %s has %s pilots, %s queued. Will submit 1 testing pilot' %(self.apfqueue.apfqname, allpilots, inactivepilots))
                # FIXME: here I have to exit              

          
            if ready > 0:  # for PanDA ready == activated jobs
                if not self.depthboost:
                        self.log.info('calcSubmitNum: depthboost unset for apfqueue %s - defaulting to 2' %self.apfqueue.apfqname) 
                        self.depthboost = 2
                if inactivepilots < self.nqueue or ( ready > inactivepilots and inactivepilots < self.nqueue * self.depthboost ):
                        self.log.info('calcSubmitNum: %d activated jobs, %d inactive pilots queued (< queue depth %d * depth boost %d). Will submit full pilot load.' %(ready, inactivepilots, self.nqueue, self.depthboost))
                        out = self.nqueue
                else:
                        self.log.info('calcSubmitNum: %d activated jobs, %d inactive pilots queued (>= queue depth %d * depth boost %d). No extra pilots needed.' %(ready, inactivepilots, self.nqueue, self.depthboost))  
                # FIXME: here I have to exit              
               
            # no activated jobs
            if inactivepilots < self.nqueue:
                # FIXME : how to deal with idlepilotsuppression 
                pass
            else:
                self.log.info('calcSubmitNum: No activated jobs, %d inactive pilots queued (queue depth %d). No extra pilots needed.' %(inactivepilots, self.nqueue))  


        self.log.debug('calcSubmitNum: Leaving returning %s' %out)
        return out
