#! /usr/bin/env python

import logging
import threading

from autopyfactory.factory import WMSStatusInterface
from autopyfactory.factory import Singleton 

import userinterface.Client as Client



class WMSStatus(threading.Thread, WMSStatusInterface):
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

        def __init__(self):
                self.log = logging.getLogger("main.pandawmsstatusplugin")
                self.log.debug("PandaWMSStatusPlugin initializing...")

                threading.Thread.__init__(self) # init the thread
                self.stopevent = threading.Event()
                # to avoid the thread to be started more than once
                self.__started = False 

        def getCloudInfo(self, cloud):
                '''
                '''
                if not self.clouds_err:
                        return self.all_clouds_config[cloud]
                
        def getSiteInfo(self, site):
                '''
                '''
                if not self.sites_err:
                        return self.all_sites_config[site]

        def getJobsInfo(self, site):
                '''
                '''
                if not self.jobs_err:
                        return self.all_sites_config[site]
       
        def start(self):
                '''
                we override method start to prevent the thread
                to be started more than once
                '''
                if not self.__started:
                        self.__started = True
                        threading.Thread.start(self)

        def run(self):                
                '''
                Main loop
                '''
                while not self.stopevent.isSet():
                        self.__upate()
                        self.__sleep()

        def __update(self):
                self.clouds_error, self.all_clouds_config = Client.getCloudSpecs()
                self.sites_err, self.all_sites_config = Client.getSiteSpecs(siteType='all')
                self.jobs_err, self.all_sites_config = Client.getJobStatisticsPerSite(countryGroup='',workingGroup='')

        def __sleep(self):
                # FIXME
                time.sleep(100)

        def join(self,timeout=None):
               '''
               stops the thread.
               '''
               #self.log.debug('[%s] Stopping thread...' % self.siteid )
               self.stopevent.set()
               threading.Thread.join(self, timeout)


