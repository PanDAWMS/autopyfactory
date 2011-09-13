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

                # variable to check if the source of information 
                # have been queried at least once
                self.updated = False

                # variable to record when was last time info was updated
                # the info is recorded as seconds since epoch
                self.lasttime = 0

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

                # if there is not any info yet available, return an empty dictionary
                if not self.updated:
                        self.log.debug('getCloudInfo: no info yet')
                        return {} 

                if maxtime > 0 and (int(time.time()) - self.lasttime) > maxtime:
                        # if info is too old, return an empty dictionary
                        self.log.debug('getCloudInfo: info too old')
                        out = {}
                else:
                        if not self.clouds_err:
                                out = self.all_clouds_config.get(cloud, {})
                        else:
                                out = {}

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

                # if there is not any info yet available, return an empty dictionary
                if not self.updated:
                        self.log.debug('getSiteInfo: no info yet')
                        return {}

                if maxtime > 0 and (int(time.time()) - self.lasttime) > maxtime:
                        self.log.debug('getSiteInfo: info too old')
                        # if info is too old, return an empty dictionary
                        out = {}
                else:
                        if not self.sites_err:
                                out = self.all_sites_config.get(site, {})
                        else:
                                out = {}

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

                # if there is not any info yet available, return an empty dictionary
                if not self.updated:
                        self.log.debug('getJobsInfo: no info yet')
                        return {}

                if maxtime > 0 and (int(time.time()) - self.lasttime) > maxtime:
                        # if info is too old, return an empty dictionary
                        self.log.debug('getJobsInfo: info too old')
                        out = {}
                else:
                        if not self.jobs_err:
                                out = self.all_jobs_config.get(site, {})
                        else:
                                out = {}
       
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

                # get Clouds Specs
                self.clouds_err, self.all_clouds_config = Client.getCloudSpecs()
                if self.clouds_err:
                        self.log.error('Client.getCloudSpecs() failed')

                # get Sites Specs
                self.sites_err, self.all_sites_config = Client.getSiteSpecs(siteType='all')
                if self.sites_err:
                        self.log.error('Client.getSiteSpecs() failed')
                # get Jobs Specs
                self.jobs_err, self.all_jobs_config = Client.getJobStatisticsPerSite(countryGroup='',workingGroup='')
                if self.jobs_err:
                        self.log.error('Client.getJobStatisticsPerSite() failed')

                self.updated = True
                self.lasttime = int(time.time())

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


