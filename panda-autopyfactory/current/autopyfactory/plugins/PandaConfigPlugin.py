#! /usr/bin/env python

import logging
import re
import threading
import time

from urllib import urlopen

from autopyfactory.info import BaseInfo 
from autopyfactory.info import InfoContainer
from autopyfactory.interfaces import ConfigInterface
from autopyfactory.factory import Singleton
from autopyfactory.configloader import Config, ConfigManager

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class SchedConfigInfo(BaseInfo):
    valid = ['batchsubmit.gram.queue', 
             'batchsubmit.gram.globusrsladd', 
             'batchsubmit.environ', 
             'batchsubmit.gridresource',
             'batchsubmit.webservice',
             'batchsubmit.queue',
             'batchsubmit.port',
             'batchsubmit.batch',
             'wmsqueue',
             ]
     # Some of these variables are intended to be used by GRAM-related 
     # submit plugins (e.g. CondorGT2, CondorGT5)
     # Other variables are meant to be consumed by CREAM submit plugins.
     # It happens the same varible is included twice, once per each case.

 
    def __init__(self):
        super(SchedConfigInfo, self).__init__(None) 


class PandaConfigPlugin(threading.Thread, ConfigInterface):
    '''
    -----------------------------------------------------------------------
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from ConfigInterface
    -----------------------------------------------------------------------
    '''

    __metaclass__ = Singleton

    def __init__(self, apfqueue):

        self._valid = True

        self.mapping_gram = {
                'special_par': 'batchsubmit.gram.globusrsladd',
                'localqueue' : 'batchsubmit.gram.queue',
                'environ'    : 'batchsubmit.environ',
                'queue'      : 'batchsubmit.gridresource',
                'siteid'     : 'wmsqueue',
                }

        self.mapping_cream = {
                'localqueue' : 'batchsubmit.queue',
                }

        try:

            self.apfqname = apfqueue.apfqname
            self.log = logging.getLogger("main.scconfigplugin[%s]" %apfqueue.apfqname)
            self.log.debug("scconfigplugin: Initializing object...")

            self.qcl = apfqueue.factory.qcl
            self.fcl = apfqueue.factory.fcl

            self.batchqueue = self.qcl.generic_get(self.apfqname, 'batchqueue', logger=self.log)
            self.url = self.qcl.generic_get(self.apfqname, 'config.panda.url', logger=self.log)
            self.sleeptime = self.fcl.generic_get('Factory', 'wmsstatus.config.sleep', 'getint', default_value=100)

            self.configsinfo = None

            # current WMSStatusIfno object
            self.currentinfo = None
            
            threading.Thread.__init__(self) # init the thread
            self.stopevent = threading.Event()
            # to avoid the thread to be started more than once
            self._started = False
            
            self.log.info('scconfigplugin: Object initialized.')
        except:
            self._valid = False


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
            self._update()
            time.sleep(self.sleeptime)
        self.log.debug('run: Leaving.')


    def join(self,timeout=None):
        '''
        stops the thread.
        '''

        self.log.debug('join: Starting with input %s' %timeout)
        self.stopevent.set()
        threading.Thread.join(self, timeout)
        self.log.debug('join: Leaving.')

    def getInfo(self):
        return self.configsinfo

    def _update(self):
        ''' 
        queries PanDA Sched Config for batchqueue info
        ''' 
        self.log.debug('_update: Starting')

        try:
            import json as json
        except ImportError, err:
            self.log.warning('_getschedconfig: json package not installed. Trying to import simplejson as json')
            import simplejson as json

        try:
            self.log.debug('_update: Make info object.')
            new_configsinfo = InfoContainer('configs', SchedConfigInfo())
            self.log.debug('_update: Made info object.')
            url = self.url
            self.log.debug('_update: Downloading data from %s' % url)
            handle = urlopen(url)
            self.log.debug('_update: Parsing json data...')
            jsonData = json.load(handle, 'utf-8')
            handle.close()
            # The following returns a huge amount of data. Enable manually if it is critical to have
            # this in the debug logs...
            # self.log.debug('_update: JSON returned: %s' % jsonData)
            
            # json always gives back unicode strings (eh?) - convert unicode to utf-8
            for batchqueue, config in jsonData.iteritems():
                if isinstance(batchqueue, unicode):
                    batchqueue = batchqueue.encode('utf-8')
                scinfo = SchedConfigInfo()
                new_configsinfo[batchqueue] = scinfo

                factoryData = {}
                for k, v in config.iteritems():
                    if isinstance(k, unicode):
                        k = k.encode('utf-8')
                    if isinstance(v, unicode):
                        v = v.encode('utf-8')
                    v = str(v)
                    if v != 'None':
                        factoryData[k] = v
                # FIXME: too much content. Recover it when we have log.trace()
                #self.log.debug('_update: content in %s for %s converted to: %s' % (url, batchqueue, factoryData))
                scinfo.fill(factoryData, self.mapping_gram)
                scinfo.fill(factoryData, self.mapping_cream)
            
                # ---------------------------------------------------------
                # code for CREAM. 
                # ---------------------------------------------------------
                # In case of CREAM, some parsing and regex is needed 
                queue = factoryData.get('queue', None)
                if queue:
                    # search for string "cream" within the content of 'queue'
                    match1 = re.match(r'([^/]+)/cream-(\w+)', queue)
                    if match1 != None:
                        newfactoryData = {}
                        # See if the port is explicitly given - if not assume 8443
                        # Currently condor needs this specified in the JDL
                        match2 = re.match(r'^([^:]+):(\d+)$', match1.group(1))
                        if match2:
                            newfactoryData['batchsubmit.webservice'] =  match2.group(1)
                            newfactoryData['batchsubmit.port'] =  match2.group(2)
                        else:
                            newfactoryData['batchsubmit.webservice'] = match1.group(1)
                            newfactoryData['batchsubmit.port']=  '8443'
                        newfactoryData['batchsubmit.batch'] = match1.group(2)
                        scinfo.fill(newfactoryData)
                # ---------------------------------------------------------
            # if everything went OK, we replace the old configsinfo variable with the new one
            self.configsinfo = new_configsinfo

        except ValueError, err:
            self.log.error('_update: %s  downloading from %s' % (err, url))
        except IOError, (errno, errmsg):
            self.log.error('_update: %s downloading from %s' % (errmsg, url))
        except Exception as e:
            self.log.error('Exception caught: %s' % e)

        self.log.debug('_update: Leaving')

