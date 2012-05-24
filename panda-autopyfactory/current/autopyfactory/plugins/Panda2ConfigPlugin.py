#! /usr/bin/env python

import logging

from urllib import urlopen

from autopyfactory.info import BaseInfo 
from autopyfactory.info import InfoContainer
from autopyfactory.factory import ConfigInterface
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
    #valid = ['batchsubmit.condorgram.gram.queue', 
    #         'batchsubmit.condorgram.gram.globusrsladd', 
    #         'batchsubmit.condor_attributes',
    #         'batchsubmit.environ', 
    #         'batchsubmit.gridresource']
    valid = ['batchsubmit.condorgram.gram.queue', 
             'batchsubmit.environ', 
             'batchsubmit.gridresource']
 
    def __init__(self):
        super(SchedConfigInfo, self).__init__(None) 


class PandaConfigPlugin(ConfigInterface):
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

        #self.mapping = {
        #        'special_par': 'batchsubmit.condorgram.gram.globusrsladd',
        #        'localqueue': 'batchsubmit.condorgram.gram.queue',
        #        'jdladd' : 'batchsubmit.condor_attributes',
        #        'environ': 'batchsubmit.environ',
        #        'queue': 'batchsubmit.gridresource',
        #        }
        self.mapping = {
                'localqueue': 'batchsubmit.condorgram.gram.queue',
                'environ': 'batchsubmit.environ',
                'queue': 'batchsubmit.gridresource',
                }

        try:

            self.apfqname = apfqueue.apfqname
            self.log = logging.getLogger("main.scconfigplugin[%s]" %apfqueue.apfqname)
            self.log.debug("scconfigplugin: Initializing object...")

            self.qcl = apfqueue.factory.qcl
            self.batchqueue = self.qcl.generic_get(self.apfqname, 'batchqueue', logger=self.log)

            self.configsinfo = InfoContainer('configs', SchedConfigInfo())
            #self.scinfo = SchedConfigInfo()

            self.log.info('scconfigplugin: Object initialized.')
        except:
            self._valid = False

    def valid(self):
        return self._valid

    def getConfig(self):
        '''
        returns a Config object with the info we are interested in
        id is the string that identifies a given class (e.g. condorgt2, condorcream...)
        '''

        self.log.debug('getConfig: Leaving')

        self._getschedconfig() 

        conf = self.scinfo.getConfig(self.apfqname) 
        ###conf.filterkeys('batchsubmit', 'batchsubmit.%s' %id)
 
        self.log.debug('getConfig: Leaving')
        return conf 

    def _getschedconfig(self):
        ''' 
        queries PanDA Sched Config for batchqueue info
        ''' 
        self.log.debug('_getschedconfig: Starting')

        try:
            import json as json
        except ImportError, err:
            self.log.warning('_getschedconfig: json package not installed. Trying to import simplejson as json')
            import simplejson as json

        try:


            url = 'http://pandaserver.cern.ch:25080/cache/schedconfig/schedconfig.all.json'
            handle = urlopen(url)
            jsonData = json.load(handle, 'utf-8')
            handle.close()
            self.log.info('_getschedconfig: JSON returned: %s' % jsonData)
            # json always gives back unicode strings (eh?) - convert unicode to utf-8
            for batchqueue, config in jsonData.iteritems():
                if isinstance(batchqueue, unicode):
                    batchqueue = batchqueue.encode('utf-8')
                    scinfo = SchedConfigInfo()
                    self.configsinfo[batchqueue] = scinfo

                    for k, v in config.iteritems():
                        factoryData = {}
                        if isinstance(k, unicode):
                            k = k.encode('utf-8')
                        if isinstance(v, unicode):
                            v = v.encode('utf-8')
                        v = str(v)
                        if v != 'None':
                            factoryData[k] = v
                            scinfo.fill(factoryData, self.mapping)
        

        #    self.log.debug('_getschedconfig: Converted to: %s' % factoryData)
        #except ValueError, err:
        #    self.log.error('_getschedconfig: %s for queue %s, downloading from %s' % (err, self.batchqueue, url))
        #except IOError, (errno, errmsg):
        #    self.log.error('_getschedconfig: %s for queue %s, downloading from %s' % (errmsg, self.batchqueue, url))

        self.log.debug('_getschedconfig: Leaving')

