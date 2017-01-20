#! /usr/bin/env python
# Added to support running module as script from arbitrary location. 
from os.path import dirname, realpath, sep, pardir
fullpathlist = realpath(__file__).split(sep)
prepath = sep.join(fullpathlist[:-4])
import sys
sys.path.insert(0, prepath)


import logging
import threading
import time
import traceback

from urllib import urlopen

from autopyfactory.interfaces import WMSStatusInterface
from autopyfactory.info import WMSStatusInfo
from autopyfactory.info import WMSQueueInfo
from autopyfactory.info import WMSStatusInfo
from autopyfactory.info import SiteInfo
from autopyfactory.info import CloudInfo
import autopyfactory.utils as utils

#libs = ("pandaclient.Client", "pandaserver.userinterface.Client", "userinterface.Client")
#for lib in libs:
#    try:
#        Client = __import__(lib, globals(), locals(), ["Client"])
#        break
#    except:
#        pass
#else:
#    raise Exception 

import autopyfactory.external.panda.Client as Client



class _panda(threading.Thread, WMSStatusInterface):
    '''
    -----------------------------------------------------------------------
    PanDA-flavored version of WMSStatus class.
    It queries the PanDA server to check the status
    of the clouds, the sites and the jobs queues.
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from Thread and from WMSStatusInterface
    -----------------------------------------------------------------------
    '''

    def __init__(self, apfqueue, config, section):
        # NOTE:
        # the **kw is not needed at this time,
        # but we use it to keep compatibility with WMS Status Condor plugin
        # However, it would allow for more than one PanDA server.

        try:
            self.apfqueue = apfqueue
            self.log = logging.getLogger("main.pandawmsstatusplugin[%s]" %apfqueue.apfqname)
            self.log.trace("WMSStatusPlugin: Initializing object...")
            self.maxage = self.apfqueue.fcl.generic_get('Factory', 'wmsstatus.panda.maxage', default_value=360)
            self.sleeptime = self.apfqueue.fcl.getint('Factory', 'wmsstatus.panda.sleep')

            # current WMSStatusIfno object
            self.currentcloudinfo = None
            self.currentjobinfo = None
            self.currentsiteinfo = None


            threading.Thread.__init__(self) # init the thread
            self.stopevent = threading.Event()
            # to avoid the thread to be started more than once
            self._started = False 

            # Using the Squid Cache when contacting the PanDA server
            Client.useWebCache()

            self.log.info('WMSStatusPlugin: Object initialized.')
        except Exception, ex:
            self.log.error("WMSStatusPlugin object initialization failed. Raising exception")
            raise ex
        # Using the Squid Cache when contacting the PanDA server
        Client.useWebCache()


    def getInfo(self, queue=None):
        '''
        Returns current WMSStatusInfo object
    
        If the info recorded is older than that maxage,
        None is returned, 
        
        '''
        self.log.trace('get: Starting with inputs maxtime=%s' % self.maxage)
        if self.currentjobinfo is None:
            self.log.trace('Info not initialized. Return None.')
            return None    
        elif self.maxage > 0 and (int(time.time()) - self.currentjobinfo.lasttime) > self.maxage:
            self.log.trace('Info is too old. Maxage = %d. Returning None' % self.maxage)
            return None    
        else:
            if queue:
                return self.currentjobinfo[queue]
            else:
                self.log.trace('Leaving. Returning info with %d items' %len(self.currentjobinfo))
                return self.currentjobinfo


    def getCloudInfo(self, cloud=None):
        '''
        selects the entry corresponding to cloud
        from the info retrieved from the PanDA server (as a dict)
        using method userinterface.Client.getCloudSpecs()

        '''
        if self.currentcloudinfo is None:
            self.log.trace('Info not initialized. Return None.')
            return None    
        elif self.maxage > 0 and (int(time.time()) - self.currentcloudinfo.lasttime) > self.maxage:
            self.log.trace('Info is too old. Maxage = %d. Returning None' % self.maxage)
            return None    
        else:
            if cloud:
                return self.currentcloudinfo[cloud]
            else:
                self.log.trace('getInfo: Leaving. Returning info with %d items' %len(self.currentcloudinfo))
                return self.currentcloudinfo
            
    def getSiteInfo(self, site=None):
        '''
        selects the entry corresponding to sites
        from the info retrieved from the PanDA server (as a dict)
        using method userinterface.Client.getSiteSpecs(siteType='all')
        '''
        if self.currentsiteinfo is None:
            self.log.trace('Info not initialized. Return None.')
            return None    
        elif self.maxage > 0 and (int(time.time()) - self.currentsiteinfo.lasttime) > self.maxage:
            self.log.trace('Info is too old. Maxage = %d. Returning None' % self.maxage)
            return None    
        else:
            if site:
                return self.currentsiteinfo[site]
            else:
                self.log.trace('getInfo: Leaving. Returning info with %d items' %len(self.currentsiteinfo))
                return self.currentsiteinfo

    def start(self):
        '''
        we override method start to prevent the thread
        to be started more than once
        '''

        self.log.trace('Staring.')

        if not self._started:
                self._started = True
                threading.Thread.start(self)

        self.log.trace('Leaving.')

    def run(self):                
        '''
        Main loop
        '''

        self.log.trace('Starting.')
        while not self.stopevent.isSet():
            try:                       
                self._update()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
            time.sleep(self.sleeptime)
        self.log.trace('Leaving.')

    def _getmaxtimeinfo(self, infotype, maxtime):
        '''
        Grab requested info with maxtime. 
        Returns info if OK, None otherwise. 
        '''
        self.log.trace('Start. infotype = %s, maxtime = %d' % (infotype, maxtime))
        out = None
        now = int(time.time())
        delta = now - self.currentinfo.lasttime
        
        if infotype in ['jobs','cloud','site']:
            if delta < maxtime:
                attrname = 'current%sinfo' % infotype
                out = getattr(attrname)
            else:
                self.log.info("_getMaxtimeinfo: Info too old. Delta is %d maxtime is %d" % (delta,maxtime))
        self.log.trace('Leaving.')        
        return out

    def _update(self):
        '''
        Queries the PanDA server for updated information about
                - Clouds configuration
                - Sites configuration
                - Jobs status per site
        '''
        self.log.trace('Starting.')
        
        try:
            newcloudinfo = self._updateclouds()
            if newcloudinfo:
                newcloudinfo.lasttime = int(time.time())

            newsiteinfo = self._updatesites()
            if newsiteinfo:
                newsiteinfo.lasttime = int(time.time())

            newjobinfo = self._updatejobs()
            if newjobinfo:
                newjobinfo.lasttime = int(time.time())
            
            self.log.debug("Replacing old info with newly generated info.")
            self.currentjobinfo = newjobinfo
            self.currentcloudinfo = newcloudinfo
            self.currentsiteinfo = newsiteinfo
        
        except Exception, e:
            self.log.error("Exception: %s" % str(e))
            self.log.error("Exception: %s" % traceback.format_exc()) 

        self.log.trace('Leaving.')


    def _updateclouds(self):
        '''
        
        Client.getCloudSpecs() ->
        
        {
        'US': {   'countries': 'usatlas',
                  'dest': 'BNL_ATLAS_1',
                  'fasttrack': 'true',
                  'mcshare': 25,
                  'name': 'US',
                  'nprestage': 12000,
                  'pilotowners': '|Nurcan Ozturk|Jose Caballero|Maxim Potekhin|John R. Hover|/atlas/usatlas/Role=production|/atlas/Role=pilot|',
                  'relocation': '',
                  'server': 'pandasrv.usatlas.bnl.gov',
                  'sites': [   'BNL_ATLAS_1',
                               'BU_ATLAS_Tier2o',
                               'BU_Atlas_Tier2o_Install',
                               'AGLT2_Install',
                               'IllinoisCC',
                               'SLACXRD_LMEM',
                               'OU_OCHEP_SWT2_Install',
                               'BNL_ATLAS_Install',
                               'SLACXRD',
                               'UTA_SWT2',
                               'UTA_SWT2_CVMFS',
                               'MWT2_UC_Install',
                               'BNL_ITB',
                               'IU_OSG',
                               'UC_ATLAS_MWT2_Install',
                               'OU_OSCER_ATLAS_Install',
                               'MWT2',
                               'Nebraska-Lincoln-red',
                               'BELLARMINE-ATLAS-T3',
                               'UC_ATLAS_MWT2',
                               'BNL_T3',
                               'SWT2_CPB',
                               'UC_ITB',
                               'UTA_SWT2_Install',
                               'HU_ATLAS_Tier2',
                               'IllinoisHEP',
                               'WT2_Install',
                               'HU_ATLAS_Tier2_Install',
                               'BNL_XRD',
                               'OU_OSCER_ATLAS_MPI',
                               'Nebraska-Lincoln-red_Install',
                               'UTD-HEP_Install',
                               'IllinoisHEP_Install',
                               'BNL_ITB_Install',
                               'IU_OSG_Install',
                               'Tufts_ATLAS_Tier3',
                               'OU_OCHEP_SWT2',
                               'MP_IllinoisHEP',
                               'Tufts_ATLAS_Tier3_Install',
                               'BNL_CVMFS_1',
                               'BNL_ATLAS_2',
                               'Nebraska-Omaha-ffgrid_Install',
                               'AGLT2',
                               'SWT2_CPB_Install',
                               'GLOW-ATLAS_Install',
                               'MWT2_UC',
                               'UTD-HEP',
                               'GLOW-ATLAS',
                               'Nebraska-Omaha-ffgrid',
                               'OU_OSCER_ATLAS',
                               'Hampton_T3'],
                      'source': 'BNL_ATLAS_1',
                      'status': 'online',
                      'tier1': 'BNL_ATLAS_1',
                      'tier1SE': [   'BNLDISK',
                                     'BNLTAPE',
                                     'BNLPANDA',
                                     'BNL-OSG2_MCDISK',
                                     'BNL-OSG2_MCTAPE',
                                     'BNL-OSG2_DATADISK',
                                     'BNL-OSG2_DATATAPE',
                                     'BNL-OSG2_HOTDISK'],
                      'transtimehi': 1,
                      'transtimelo': 4,
                      'validation': 'true',
                      'waittime': 0,
                      'weight': 2}
            }            

        '''

        before = time.time()
        # get Clouds Specs
        clouds_err, all_clouds_config = Client.getCloudSpecs()
        delta = time.time() - before
        self.log.trace('it took %s seconds to perform the query' %delta)
        self.log.debug('%s seconds to perform query' %delta)
        out = None
        if clouds_err:
            self.log.error('Client.getCloudSpecs() failed')
            return None
        else:
            cloudsinfo = WMSStatusInfo()
            for cloud in all_clouds_config.keys():
                    ci = CloudInfo()
                    cloudsinfo[cloud] = ci
                    attrdict = all_clouds_config[cloud]
                    ci.fill(attrdict)
            return cloudsinfo
                        

    def _updatesites(self):
        '''
        Client.getSiteSpecs(siteType='all')   ->
        
        {
        'BNL_ATLAS_1': {   'accesscontrol': '',
                           'allowdirectaccess': False,
                           'allowedgroups': '',
                           'cachedse': '',
                           'cloud': 'US',
                           'cloudlist': ['US'],
                           'cmtconfig': [   'i686-slc4-gcc34-opt',
                                            'i686-slc5-gcc43-opt'],
                           'comment': 'ELOG.31117',
                           'copysetup': '/usatlas/OSG/osg_wn_client/current/setup.sh',
                           'ddm': 'BNL-OSG2_DATADISK',
                           'defaulttoken': '',
                           'dq2url': '',
                           'gatekeeper': 'gridgk03.racf.bnl.gov',
                           'glexec': '',
                           'lfchost': 'lfc.usatlas.bnl.gov',
                           'lfcregister': '',
                           'localqueue': '',
                           'maxinputsize': 60000,
                           'maxtime': 0,
                           'memory': 0,
                           'nickname': 'BNL_ATLAS_1-condor',
                           'priorityoffset': '',
                           'queue': 'gridgk03.racf.bnl.gov/jobmanager-condor',
                           'releases': [   '10.0.1',
                                           '10.0.4',
                                           '11.0.0',
                                           '11.0.1',
                                           '11.0.2',
                                           '11.0.3',
                                           '11.0.4',
                                           '11.0.42',
                                           '11.0.5',
                                           '11.3.0',
                                           '11.5.0',
                                           '12.0.0',
                                           '12.0.1',
                                           '12.0.2',
                                           '12.0.3',
                                           '12.0.31',
                                           '12.0.4',
                                           '12.0.5',
                                           '12.0.6',
                                           '12.0.7',
                                           '12.0.8',
                                           '12.0.95',
                                           '13.0.10',
                                           '13.0.20',
                                           '13.0.25',
                                           '13.0.25-slc3',
                                           '13.0.26',
                                           '13.0.30',
                                           '13.0.35',
                                           '13.0.30',
                                           '13.0.35',
                                           '13.0.35-slc3',
                                           '13.0.40',
                                           '13.2.0',
                                           '14.0.0',
                                           '14.0.10',
                                           '14.1.0',
                                           '14.2.0',
                                           '14.2.10',
                                           '14.2.11',
                                           '14.2.20',
                                           '14.2.20.bak',
                                           '14.2.21',
                                           '14.2.22',
                                           '9.0.4'],
                           'retry': False,
                           'se': 'token:ATLASDATADISK:srm://dcsrm.usatlas.bnl.gov:8443/srm/managerv2?SFN=',
                           'seprodpath': {   'ATLASDATADISK': '/pnfs/usatlas.bnl.gov/BNLT0D1/',
                                             'ATLASDATATAPE': '/pnfs/usatlas.bnl.gov/BNLT1D0/',
                                             'ATLASMCTAPE': '/pnfs/usatlas.bnl.gov/MCTAPE/'},
                           'setokens': {   'ATLASDATADISK': 'BNL-OSG2_DATADISK',
                                           'ATLASDATATAPE': 'BNL-OSG2_DATATAPE',
                                           'ATLASMCTAPE': 'BNL-OSG2_MCTAPE'},
                           'sitename': 'BNL_ATLAS_1',
                           'space': 481594,
                           'status': 'offline',
                           'statusmodtime': datetime.datetime(2011, 10, 18, 10, 45, 44),
                           'type': 'production',
                           'validatedreleases': ['True']},
        }
        
        -------------------------------------------------------------------------------------------------------

        $ curl --connect-timeout 20 --max-time 180 -sS 'http://panda.cern.ch:25980/server/pandamon/query?autopilot=queuedata&nickname=TEST2&pandasite=TEST2'
        
        (lp1
        (dp2
        S'gatekeeper'
        p3
        S'gridgk01.racf.bnl.gov' 
            ...
            ...
        )

        -------------------------------------------------------------------------------------------------------
        
        This is what APF 1.X is getting from SchedConfig:

                http://pandaserver.cern.ch:25085/cache/schedconfig/ANALY_BNL_ATLAS_1-condor.factory.json
                {
                    "cloud": "US",
                    "depthboost": null,
                    "environ": "APP=/usatlas/OSG TMP=/tmp DATA=/usatlas/prodjob/share/",
                    "glexec": null,
                    "idlepilotsupression": null,
                    "jdl": "ANALY_BNL_ATLAS_1-condor",
                    "localqueue": null,
                    "maxtime": 0,
                    "memory": 0,
                    "nickname": "ANALY_BNL_ATLAS_1-condor",
                    "nqueue": 300,
                    "pilotlimit": null,
                    "proxy": "donothide",
                    "queue": "gridgk05.racf.bnl.gov/jobmanager-condor",
                    "site": "BNL",
                    "siteid": "ANALY_BNL_ATLAS_1",
                    "status": "online",
                    "system": "osg",
                    "transferringlimit": null
                }
                
            '''
        before = time.time()
        # get Sites Specs from Client.py
        sites_err, all_sites_config = Client.getSiteSpecs(siteType='all')
        delta = time.time() - before

        self.log.trace('_updateSites: it took %s seconds to perform the query' %delta)
        self.log.debug('_updateSites: %s seconds to perform query' %delta)
        out = None
        if sites_err:
            self.log.error('Client.getSiteSpecs() failed.')
            return None
        else:
            sitesinfo = WMSStatusInfo()
            for site in all_sites_config.keys():
                    si = SiteInfo()
                    sitesinfo[site] = si
                    attrdict = all_sites_config[site]
                    si.fill(attrdict)
            return sitesinfo

                        
    def _updatejobs(self):
        '''
        Client.getJobStatisticsPerSite(
                    countryGroup='',
                    workingGroup='', 
                    jobType='test,prod,managed,user,panda,ddm,rc_test'
                    )  ->
        
        {   None: {   'activated': 0,
                      'assigned': 0,
                      'cancelled': 11632,
                      'defined': 2196,
                      'failed': 0,
                      'finished': 0,
                      'running': 0},
           
           'AGLT2': { 'activated': 495,
                      'assigned': 170,
                      'cancelled': 1,
                      'failed': 15,
                      'finished': 114,
                      'holding': 9,
                      'running': 341,
                      'starting': 1,
                      'transferring': 16},
        }

        Client.getJobStatisticsWithLabel() ->

        {'FZK-LCG2': {'prod_test': {'activated': 8, 
                                    'holding': 1 },
                        'managed': {'assigned': 98, 
                                    'running': 3541, 
                                    'transferring': 135, 
                                    'activated': 6684, 
                                    'holding': 70 },
                        'rc_test': {'activated': 1}
                      },
         'BU_ATLAS_Tier2o': {'prod_test': {'running': 8, 
                                           'activated': 1, 
                                           'holding': 6},
                               'managed': {'defined': 33, 
                                           'transferring': 262, 
                                           'activated': 1362, 
                                           'assigned': 10, 
                                           'running': 746, 
                                           'holding': 7},
                                 'rc_test': {'activated': 2}
                             }
          }

        '''
        
        
        before = time.time()
        # get Jobs Specs
        #jobs_err, all_jobs_config = Client.getJobStatisticsPerSite(
        #            countryGroup='',
        #            workingGroup='', 
        #            jobType='test,prod,managed,user,panda,ddm,rc_test,prod_test'
        #            ) 
        jobs_err, all_jobs_config = Client.getJobStatisticsWithLabel()
        # NOTE: reason to use getJobStatisticsWithLabel()
        #       is because by default PanDA does not give info on all labels.
        #       Jobs info for labels like "rc-test" is hidden,
        #       so we ask explicitly for all labels.
                                                                                   
        delta = time.time() - before
        self.log.debug('_updateJobs: %s seconds to perform query' %delta)
        out = None

        if jobs_err:
                self.log.error('Client.getJobStatisticsPerSite() failed.')
                return None 
                
        self.jobsstatisticspersite2info = self.apfqueue.factory.mappingscl.section2dict('PANDAWMSSTATUS-JOBSSTATISTICSPERSITE2INFO')
        self.log.trace('jobsstatisticspersite2info mappings are %s' %self.jobsstatisticspersite2info)
        ###self.jobsstatisticspersite2info = {'pending'     : 'notready',
        ###                                   'defined'     : 'notready',
        ###                                   'assigned'    : 'notready',
        ###                                   'waiting'     : 'notready',
        ###                                   'activated'   : 'ready',
        ###                                   'starting'    : 'running',
        ###                                   'sent'        : 'running',
        ###                                   'running'     : 'running',
        ###                                   'holding'     : 'running',
        ###                                   'transferring': 'running',
        ###                                   'finished'    : 'done',
        ###                                   'failed'      : 'failed',
        ###                                   'cancelled'   : 'failed'}

        wmsstatusinfo = WMSStatusInfo()
        for wmssite in all_jobs_config.keys():
                qi = WMSQueueInfo()
                wmsstatusinfo[wmssite] = qi
                for label in all_jobs_config[wmssite].keys():
                    attrdict = all_jobs_config[wmssite][label] 
                    qi.fill(attrdict, mappings=self.jobsstatisticspersite2info, reset=False)
        return wmsstatusinfo

    def join(self,timeout=None):
        '''
        stops the thread.
        '''
        self.log.trace('Starting with input %s' %timeout)
        self.stopevent.set()
        threading.Thread.join(self, timeout)
        self.log.trace('Leaving.')


# =============================================================================
#       Singleton wrapper
# =============================================================================

class Panda(object):

    instance = None

    def __new__(cls, *k, **kw):
        if not Panda.instance:
            Panda.instance = _panda(*k, **kw)
        return Panda.instance
        

# =============================================================================

def runstandalone():
    print("Running standalone...")


if __name__=='__main__':
    runstandalone()

            
