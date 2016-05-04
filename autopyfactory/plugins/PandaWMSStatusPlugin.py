#! /usr/bin/env python

import logging
import threading
import time
import traceback

from urllib import urlopen

from autopyfactory.factory import WMSStatusInterface
from autopyfactory.factory import WMSStatusInfo
from autopyfactory.factory import Singleton 
from autopyfactory.info import InfoContainer
from autopyfactory.info import WMSQueueInfo
from autopyfactory.info import SiteInfo
from autopyfactory.info import CloudInfo
import autopyfactory.utils as utils
import userinterface.Client as Client

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"


class PandaWMSStatusPlugin(threading.Thread, WMSStatusInterface):
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

    __metaclass__ = Singleton

    def __init__(self, apfqueue):
        self._valid = True
        try:
            self.apfqueue = apfqueue
            self.log = logging.getLogger("main.pandawmsstatusplugin[%s]" %apfqueue.apfqname)
            self.log.debug("WMSStatusPlugin: Initializing object...")
            self.wmsstatusmaxtime = 0
            if self.apfqueue.fcl.has_option('Factory', 'wmsstatus.maxtime'):
                self.wmsstatusmaxtime = self.fcl.get('Factory', 'wmsstatus.maxtime')
            self.sleeptime = self.apfqueue.fcl.getint('Factory', 'wmsstatus.panda.sleep')

            # current WMSStatusIfno object
            self.currentinfo = None

            threading.Thread.__init__(self) # init the thread
            self.stopevent = threading.Event()
            # to avoid the thread to be started more than once
            self._started = False 

            self.log.info('WMSStatusPlugin: Object initialized.')
        except:
            self._valid = False

    def valid(self):
        return self._valid

    def getCloudInfo(self, maxtime=0):
        '''
        selects the entry corresponding to cloud
        from the info retrieved from the PanDA server (as a dict)
        using method userinterface.Client.getCloudSpecs()

        '''
        self.log.debug('getCloudInfo: Starting maxtime = %s' %maxtime)
        out = self.currentinfo.cloud
        self.log.info('getCloudInfo: Cloud has %d entries' % len(out))
        return out 
            
    def getSiteInfo(self, maxtime=0):
        '''
        selects the entry corresponding to sites
        from the info retrieved from the PanDA server (as a dict)
        using method userinterface.Client.getSiteSpecs(siteType='all')
        '''
        self.log.debug('getSiteInfo: Starting. maxtime = %s' %maxtime)

        out = self.currentinfo.site

        self.log.info('getSiteInfo: Siteinfo has %d entries' %len(out))
        return out 

    def getJobsInfo(self, maxtime=0):
         '''
         selects the entry corresponding to jobs 
         from the info retrieved from the PanDA server (as a dict)
         using method userinterface.Client.getJobStatisticsPerSite(countryGroup='',workingGroup='')
         '''
         self.log.debug('getJobsInfo: Starting. maxtime = %s' %maxtime)
         out = self._getmaxtimeinfo('jobs', maxtime)
         self.log.info('getJobsInfo: Jobs has %d entries.' %len(out))
         return out

    def start(self):
        '''
        we override method start to prevent the thread
        to be started more than once
        '''

        self.log.debug('start: Staring.')

        if not self._started:
                self._started = True
                threading.Thread.start(self)

        self.log.debug('start: Leaving.')

    def run(self):                
        '''
        Main loop
        '''

        self.log.debug('run: Starting.')
        while not self.stopevent.isSet():
            try:                       
                self._update()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
            time.sleep(self.sleeptime)
        self.log.debug('run: Leaving.')

    def _getmaxtimeinfo(self, infotype, maxtime):
        '''
        Grab requested info with maxtime. 
        Returns info if OK, None otherwise. 
        '''
        self.log.debug('_getmaxtimeinfo: Start. infotype = %s, maxtime = %d' % (infotype, maxtime))
        out = None
        now = int(time.time())
        delta = now - self.currentinfo.lasttime
        
        if infotype in ['jobs','cloud','site']:
            if delta < maxtime:
                out = getattr(self.currentinfo, infotype)
            else:
                self.log.info("_getMaxtimeinfo: Info too old. Delta is %d maxtime is %d" % (delta,maxtime))
        self.log.debug('_getmaxtimeinfo: Leaving.')        

    def _update(self):
        '''
        Queries the PanDA server for updated information about
                - Clouds configuration
                - Sites configuration
                - Jobs status per site
        '''
        self.log.debug('_update: Starting.')
        newinfo = WMSStatusInfo()
        
        try:
            newinfo.cloud = self._updateclouds()
            newinfo.site = self._updatesites()
            newinfo.jobs = self._updatejobs()
            newinfo.lasttime = int(time.time())
            self.log.info("Replacing old info with newly generated info.")
            self.currentinfo = newinfo
        except Exception, e:
            self.log.error("_update: Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc()) 

        self.log.debug('_update: Leaving.')


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
        self.log.debug('_updateclouds: it took %s seconds to perform the query' %delta)
        self.log.info('_updateclouds: %s seconds to perform query' %delta)
        out = None
        #if not clouds_err:
        #    out = all_clouds_config 
        #else:
        #    self.log.error('Client.getCloudSpecs() failed')
        #return out
        if clouds_err:
            self.log.error('Client.getCloudSpecs() failed')
        else:
            cloudsinfo = InfoContainer('clouds', CloudInfo())
            for cloud in all_clouds_config.keys():
                    ci = CloudInfo()
                    cloudsinfo[cloud] = ci
                    attrdict = all_clouds_config[cloud]
                    ci.fill(attrdict)
            return cloudsinfo
                        
    ###   def _updatejobs(self):
    ###       '''
    ###       
    ###       Client.getJobStatisticsPerSite(
    ###                   countryGroup='',
    ###                   workingGroup='', 
    ###                   jobType='test,prod,managed,user,panda,ddm,rc_test'
    ###                   )  ->
    ###       
    ###       {   None: {   'activated': 0,
    ###                     'assigned': 0,
    ###                     'cancelled': 11632,
    ###                     'defined': 2196,
    ###                     'failed': 0,
    ###                     'finished': 0,
    ###                     'running': 0},
    ###          
    ###          'AGLT2': { 'activated': 495,
    ###                     'assigned': 170,
    ###                     'cancelled': 1,
    ###                     'failed': 15,
    ###                     'finished': 114,
    ###                     'holding': 9,
    ###                     'running': 341,
    ###                     'starting': 1,
    ###                     'transferring': 16},
    ###       }
    ###       '''
    ###       
    ###       
    ###       before = time.time()
    ###       # get Jobs Specs
    ###       #self.jobs_err, self.all_jobs_config = Client.getJobStatisticsPerSite(countryGroup='',workingGroup='') 
    ###       jobs_err, all_jobs_config = Client.getJobStatisticsPerSite(
    ###                   countryGroup='',
    ###                   workingGroup='', 
    ###                   jobType='test,prod,managed,user,panda,ddm,rc_test'
    ###                   ) 
    ###                                                                                  
    ###       delta = time.time() - before
    ###       self.log.info('_updateJobs: %s seconds to perform query' %delta)
    ###       out = None
    ###       ###if not jobs_err:
    ###       ###    out = all_jobs_config
    ###       ###else:
    ###       ###    self.log.error('Client.getJobStatisticsPerSite() failed.')
    ###       ###return out
    ###       


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

        self.log.debug('_updateSites: it took %s seconds to perform the query' %delta)
        self.log.info('_updateSites: %s seconds to perform query' %delta)
        out = None
        #if not sites_err:
        #    out = all_sites_config 
        #else:
        #    self.log.error('Client.getSiteSpecs() failed.')
        #return out     
        if sites_err:
            self.log.error('Client.getSiteSpecs() failed.')
        else:
            sitesinfo = InfoContainer('sites', SiteInfo())
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
                                                                                   
        delta = time.time() - before
        self.log.info('_updateJobs: %s seconds to perform query' %delta)
        out = None

        if jobs_err:
                self.log.error('Client.getJobStatisticsPerSite() failed.')
                return None 
                
        self.jobsstatisticspersite2info = {'pending'     : 'notready',
                                           'defined'     : 'notready',
                                           'assigned'    : 'notready',
                                           'waiting'     : 'notready',
                                           'activated'   : 'ready',
                                           'starting'    : 'running',
                                           'sent'        : 'running',
                                           'running'     : 'running',
                                           'holding'     : 'running',
                                           'transferring': 'running',
                                           'finished'    : 'done',
                                           'failed'      : 'failed',
                                           'cancelled'   : 'failed'}

        wmsqueueinfo = InfoContainer('jobs', WMSQueueInfo())
        for wmssite in all_jobs_config.keys():
                qi = WMSQueueInfo()
                wmsqueueinfo[wmssite] = qi
                #attrdict = all_jobs_config[wmssite]
                #qi.fill(attrdict, mappings=self.jobsstatisticspersite2info)
                for label in all_jobs_config[wmssite].keys():
                    attrdict = all_jobs_config[wmssite][label] 
                    qi.fill(attrdict, mappings=self.jobsstatisticspersite2info, reset=False)
        return wmsqueueinfo

    def join(self,timeout=None):
        '''
        stops the thread.
        '''
        self.log.debug('join: Starting with input %s' %timeout)
        self.stopevent.set()
        threading.Thread.join(self, timeout)
        self.log.debug('join: Leaving.')


    def getInfo(self, maxtime=0):
        '''
        Returns current WMSStatusInfo object

        Optionally, and maxtime parameter can be passed.
        In that case, if the info recorded is older than that maxtime,
        None is returned, 
        
        '''
        self.log.debug('get: Starting with inputs maxtime=%s' % maxtime)
        if self.currentinfo is None:
            self.log.debug('getInfo: Info not initialized. Return None.')
            return None    
        elif maxtime > 0 and (int(time.time()) - self.currentinfo.lasttime) > maxtime:
            self.log.debug('getInfo: Info is too old. Maxtime = %d. Returning None' % maxtime)
            return None    
        else:
            self.log.debug('getInfo: Leaving. Returning info with %d items' %len(self.currentinfo))
            return self.currentinfo
            
