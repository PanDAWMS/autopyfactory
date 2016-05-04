#! /usr/bin/env python

import logging
import threading
import time

from autopyfactory.factory import WMSStatusInterface
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

                self.info = InfoHandler()

                threading.Thread.__init__(self) # init the thread
                self.stopevent = threading.Event()
                # to avoid the thread to be started more than once
                self.__started = False 

                self.log.info('WMSStatusPlugin: Object initialized.')

        def getCloudInfo(self, cloud, maxtime=0):
                '''
                selects the entry corresponding to cloud
                from the info retrieved from the PanDA server (as a dict)
                using method userinterface.Client.getCloudSpecs()

                Optionally, and maxtime parameter can be passed.
                In that case, if the info recorded is older than that maxtime,
                an empty dictionary is returned, 
                as we understand that info is too old and most probably
                not realiable anymore.
                '''

                self.log.debug('getCloudInfo: Starting with input %s' %cloud)

                out = self.info.get(InfoHandler.CLOUDS, cloud, maxtime)

                self.log.debug('getCloudInfo: Leaving returning %s' %out)
                return out 
                
        def getSiteInfo(self, site, maxtime=0):
                '''
                selects the entry corresponding to sites
                from the info retrieved from the PanDA server (as a dict)
                using method userinterface.Client.getSiteSpecs(siteType='all')

                Optionally, and maxtime parameter can be passed.
                In that case, if the info recorded is older than that maxtime,
                an empty dictionary is returned, 
                as we understand that info is too old and most probably
                not realiable anymore.
                '''

                self.log.debug('getSiteInfo: Starting with input %s' %site)

                out = self.info.get(InfoHandler.SITES, site, maxtime)

                self.log.debug('getSiteInfo: Leaving returning %s' %out)
                return out 

        def getJobsInfo(self, site, maxtime=0):
                '''
                selects the entry corresponding to jobs 
                from the info retrieved from the PanDA server (as a dict)
                using method userinterface.Client.getJobStatisticsPerSite(countryGroup='',workingGroup='')

                Optionally, and maxtime parameter can be passed.
                In that case, if the info recorded is older than that maxtime,
                an empty dictionary is returned, 
                as we understand that info is too old and most probably
                not realiable anymore.
                '''

                self.log.debug('getJobsInfo: Starting with input %s' %site)

                out = self.info.get(InfoHandler.JOBS, site, maxtime)
       
                self.log.debug('getJobsInfo: Leaving returning %s' %out)
                return out

        def start(self):
                '''
                we override method start to prevent the thread
                to be started more than once
                '''

                self.log.debug('start: Staring.')

                if not self.__started:
                        self.__started = True
                        threading.Thread.start(self)

                self.log.debug('start: Leaving.')

        def run(self):                
                '''
                Main loop
                '''

                self.log.debug('run: Starting.')

                while not self.stopevent.isSet():
                        self.__update()
                        self.__sleep()

                self.log.debug('run: Leaving.')

        def __update(self):
                '''
                Queries the PanDA server for updated information about
                        - Clouds configuration
                        - Sites configuration
                        - Jobs status per site
                '''
                
                self.log.debug('__update: Starting.')

                before = time.time()

                # get Clouds Specs
                self.clouds_err, self.all_clouds_config = Client.getCloudSpecs()
                self.info.update(InfoHandler.CLOUDS, self.all_clouds_config, self.clouds_err)
                if self.clouds_err:
                        self.log.error('Client.getCloudSpecs() failed')

                # get Sites Specs
                self.sites_err, self.all_sites_config = Client.getSiteSpecs(siteType='all')
                self.info.update(InfoHandler.SITES, self.all_sites_config, self.sites_err)
                if self.sites_err:
                        self.log.error('Client.getSiteSpecs() failed')
                # get Jobs Specs
                #self.jobs_err, self.all_jobs_config = Client.getJobStatisticsPerSite(countryGroup='',workingGroup='') 
                self.jobs_err, self.all_jobs_config = Client.getJobStatisticsPerSite(countryGroup='',workingGroup='', jobType='test,prod,managed,user,panda,ddm,rc_test') 
                                                                                                                        # FIXME
                                                                                                                        # THIS IS A TEMPORARY SOLUTION
                                                                                                                        # THE LIST OF JOB TYPES SHOULD BE PASSED AS INPUT
                                                                                                                        # THAT LIST SHOULD BE CALCULATED:
                                                                                                                        #       - AS A PARAMETER
                                                                                                                        #       - REPORTED DYNAMICALLY BY CLIENT

                delta = time.time() - before
                self.log.debug('__update: it took %s seconds to perform the query' %delta)

                self.info.update(InfoHandler.JOBS, self.all_jobs_config, self.jobs_err)
                if self.jobs_err:
                        self.log.error('Client.getJobStatisticsPerSite() failed')

                self.log.debug('__update: Leaving.')

        def __sleep(self):
                # FIXME: temporary solution
                self.log.debug('__sleep: Starting.')
                sleeptime = self.wmsqueue.fcl.getint('Factory', 'wmsstatussleep')
                time.sleep(sleeptime)
                self.log.debug('__sleep: Leaving.')

        def join(self,timeout=None):
                '''
                stops the thread.
                '''

                self.log.debug('join: Starting with input %s' %timeout)

                self.stopevent.set()
                threading.Thread.join(self, timeout)

                self.log.debug('join: Leaving.')


class InfoHandler:
        '''
        -----------------------------------------------------------------------
        this class is just an ancilla to store and handle 
        the info that WMSStatusPlugin has to manage.
        -----------------------------------------------------------------------
        Public Interface:
                update(self, name, value, error)
                get(self, name, key, maxtime=0)
        -----------------------------------------------------------------------
        '''

        CLOUDS = 'clouds'
        SITES = 'sites'
        JOBS = 'jobs'
        
        def __init__(self):

                self.log = logging.getLogger("main.pandawmsstatusplugininfohandler") 
                self.log.info("InfoHandler: Initializing object...")

                # variable to check if the information 
                # have been introduced at least once
                self.initialized = False

                # variable to record when was last time info was updated
                # the info is recorded as seconds since epoch
                self.lasttime = 0

                # info 
                self.all_clouds_config = {}
                self.all_sites_config = {}
                self.all_jobs_config = {}
                # tmp info when there was an error
                self.err_all_clouds_config = {}
                self.err_all_sites_config = {}
                self.err_all_jobs_config = {}
                # errors
                self.clouds_err = None
                self.sites_err = None
                self.jobs_err = None

                self.log.info("InfoHandler: Object initialized.")

        def update(self, name, value, error):
                '''
                just updates the stores info
                for CLOUDS/SITES/JOBS        
                
                If there is no error, the value is stored in the regular 
                all_clouds_config/all_sites_config/all_jobs_config variables

                If there is an error, the value is stored in special variables
                err_all_clouds_config/err_all_sites_config/err_all_jobs_config 
                '''

                #self.log.debug('update: Starting with inputs name=%s; value=%s; error=%s' %(name, value, error))
                self.log.debug('update: Starting.')

                self.initialized = True
                self.lasttime = int(time.time())
                
                if name == InfoHandler.CLOUDS:
                        if not error:
                                self.all_clouds_config = value 
                        else:
                                self.err_all_clouds_config = value 
                        self.clouds_err = error              

                if name == InfoHandler.SITES:
                        if not error:
                                self.all_sites_config = value 
                        else:
                                self.err_all_sites_config = value 
                        self.sites_err = error              

                if name == InfoHandler.JOBS:
                        if not error:
                                self.all_jobs_config = value 
                        else:
                                self.err_all_jobs_config = value 
                        self.jobs_err = error              

                self.log.debug('update: Leaving.')

        def get(self, name, key, maxtime=0):
                '''
                selects the entry corresponding to clouds/sites/jobs
                from the info retrieved from the PanDA server 
                (as a set of dicts)
                using method userinterface.Client.getCloudSpecs()

                Optionally, and maxtime parameter can be passed.
                In that case, if the info recorded is older than that maxtime,
                an empty dictionary is returned, 
                as we understand that info is too old and most probably
                not realiable anymore.
                '''

                self.log.debug('get: Starting with inputs name=%s key=%s maxtime=%s.' %(name, key, maxtime))

                if not self.initialized:
                        self.log.debug('get: Info not initialized.')
                        self.log.debug('get: Leaving and return empty dictionary')
                        return {}
                if maxtime > 0 and (int(time.time()) - self.lasttime) > maxtime:
                        self.log.debug('get: Info is too old')
                        self.log.debug('get: Leaving and return empty dictionary')
                        return {}
                else:
                        if name == InfoHandler.CLOUDS:
                                out = self.all_clouds_config.get(key, {})
                        if name == InfoHandler.SITES:
                                out = self.all_sites_config.get(key, {})
                        if name == InfoHandler.JOBS:
                                out = self.all_jobs_config.get(key, {})
        
                self.log.debug('get: Leaving and returning %s' %out)
                return out

