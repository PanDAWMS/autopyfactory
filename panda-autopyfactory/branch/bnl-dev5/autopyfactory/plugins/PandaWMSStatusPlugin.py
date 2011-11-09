#! /usr/bin/env python

import logging
import threading
import time

from autopyfactory.factory import WMSStatusInterface
from autopyfactory.factory import WMSStatusInfo
from autopyfactory.factory import Singleton 

import userinterface.Client as Client

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"


class WMSStatusPlugin(threading.Thread, WMSStatusInterface):
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

    def __init__(self, wmsqueue):
        self.wmsqueue = wmsqueue
        self.log = logging.getLogger("main.pandawmsstatusplugin[%s]" %wmsqueue.apfqueue)
        self.log.info("WMSStatusPlugin: Initializing object...")
        self.wmsstatusmaxtime = 0
        if self.wmsqueue.fcl.has_option('Factory', 'wmsstatus.maxtime'):
            self.wmsstatusmaxtime = self.fcl.get('Factory', 'wmsstatus.maxtime')
        self.sleeptime = self.wmsqueue.fcl.getint('Factory', 'wmsstatus.panda.sleep')

        # current WMSStatusIfno object
        self.currentinfo = None

        threading.Thread.__init__(self) # init the thread
        self.stopevent = threading.Event()
        # to avoid the thread to be started more than once
        self._started = False 

        self.log.info('WMSStatusPlugin: Object initialized.')

    def getCloudInfo(self, maxtime=0):
        '''
        selects the entry corresponding to cloud
        from the info retrieved from the PanDA server (as a dict)
        using method userinterface.Client.getCloudSpecs()

        '''
        self.log.debug('getCloudInfo: Starting maxtime = %s' %maxtime)
        out = self.currentinfo.cloud
        self.log.debug('getCloudInfo: Leaving. Cloud has %d entries' % len(out))
        return out 
            
    def getSiteInfo(self, maxtime=0):
        '''
        selects the entry corresponding to sites
        from the info retrieved from the PanDA server (as a dict)
        using method userinterface.Client.getSiteSpecs(siteType='all')
        '''
        self.log.debug('getSiteInfo: Starting. maxtime = %s' %maxtime)

        out = self.currentinfo.site

        self.log.debug('getSiteInfo: Leaving. Siteinfo has %d entries' %len(out))
        return out 

    def getJobsInfo(self, maxtime=0):
         '''
         selects the entry corresponding to jobs 
         from the info retrieved from the PanDA server (as a dict)
         using method userinterface.Client.getJobStatisticsPerSite(countryGroup='',workingGroup='')
         '''
         self.log.debug('getJobsInfo: Starting. maxtime = %s' %maxtime)
         out = self._getmaxtimeinfo('jobs', maxtime)
         self.log.debug('getJobsInfo: Leaving. Jobs has %d entries.' %len(out))
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
                time.sleep(self.sleeptime)
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
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
                self.log.info("_getMaxtimeinfo: Info too old. Delta is %d maxtime is %d" % (delta, ))
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
            newinfo.clouds = self._updateclouds()
            newinfo.sites = self._updatesites()
            newinfo.jobs = self._updatejobs()
            self.currentinfo = newinfo
            self.currentinfo.lasttime = int(time.time())
        except Exception:
            self.log.error("Problem updating new info for WMS status.")
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
        self.clouds_err, self.all_clouds_config = Client.getCloudSpecs()
        self.info.update(InfoHandler.CLOUDS, self.all_clouds_config, self.clouds_err)
        if self.clouds_err:
            self.log.error('Client.getCloudSpecs() failed')
        delta = time.time() - before
        self.log.debug('_updateclouds: it took %s seconds to perform the query' %delta)

        
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
        
        '''
        before = time.time()
        # get Sites Specs
        self.sites_err, self.all_sites_config = Client.getSiteSpecs(siteType='all')
        self.info.update(InfoHandler.SITES, self.all_sites_config, self.sites_err)
        if self.sites_err:
            self.log.error('Client.getSiteSpecs() failed.')
                    
                
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
        '''
        
        
        before = time.time()
        # get Jobs Specs
        #self.jobs_err, self.all_jobs_config = Client.getJobStatisticsPerSite(countryGroup='',workingGroup='') 
        # FIXME
        # THIS IS A TEMPORARY SOLUTION
        # THE LIST OF JOB TYPES SHOULD BE PASSED AS INPUT
        # THAT LIST SHOULD BE CALCULATED:
        #       - AS A PARAMETER
        #       - REPORTED DYNAMICALLY BY C
        self.jobs_err, self.all_jobs_config = Client.getJobStatisticsPerSite(
                    countryGroup='',
                    workingGroup='', 
                    jobType='test,prod,managed,user,panda,ddm,rc_test'
                    ) 
                                                                                   
        delta = time.time() - before
        self.log.debug('_update: it took %s seconds to perform the query' %delta)

        self.info.update(InfoHandler.JOBS, self.all_jobs_config, self.jobs_err)
        if self.jobs_err:
                self.log.error('Client.getJobStatisticsPerSite() failed')

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
            self.log.debug('get: Starting with inputs name=%s key=%s maxtime=%s.' %(name, key, maxtime))
            out = None
            if not self.initialized:
                    self.log.debug('get: Info not initialized.')
                    self.log.debug('get: Leaving and return empty dictionary')
            elif maxtime > 0 and (int(time.time()) - self.currentinfo.lasttime) > maxtime:
                    self.log.debug('get: Info is too old')
                    self.log.debug('get: Leaving and return empty dictionary')
            else:
                    out = self.currentinfo
            self.log.debug('get: Leaving and returning %s' %out)
            return out
