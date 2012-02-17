#! /usr/bin/env python

import logging

from urllib import urlopen

from autopyfactory.factory import ConfigInterface
from autopyfactory.configloader2 import Config, ConfigManager

#from autopyfactory.factory import WMSStatusInterface
#from autopyfactory.factory import WMSStatusInfo
#from autopyfactory.info import InfoContainer

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"


class PandaConfigPlugin(ConfigInterface):
    '''
    -----------------------------------------------------------------------
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from ConfigInterface
    -----------------------------------------------------------------------
    '''


    def __init__(self, apfqueue):
        self._valid = True
        try:

            self.apfqname = apfqueue.apfqname
            self.log = logging.getLogger("main.scconfigplugin[%s]" %apfqueue.apfqname)
            self.log.debug("scconfigplugin: Initializing object...")

            self.qcl = apfqueue.factory.qcl
            self.batchqueue = self.qcl.get(self.apfqname, 'batchqueue')

            # temporary draft solution
            self.gridresource = None 
            self.queue = None 
            self._getschedconfig() 

            self.log.info('scconfigplugin: Object initialized.')
        except:
            self._valid = False

    def valid(self):
        return self._valid

    def _getschedconfig(self):
        ''' 
        ''' 
        try:
                import json as json
        except ImportError, err:
                self.log.warning('_getschedconfig: json package not installed. Trying to import simplejson as json')
                import simplejson as json

        try:
                url = 'http://pandaserver.cern.ch:25080/cache/schedconfig/%s.factory.json' % self.batchqueue
                handle = urlopen(url)
                jsonData = json.load(handle, 'utf-8')
                handle.close()
                self.log.info('_getschedconfig: JSON returned: %s' % jsonData)
                factoryData = {}
                # json always gives back unicode strings (eh?) - convert unicode to utf-8
                for k, v in jsonData.iteritems():
                        if isinstance(k, unicode):
                                k = k.encode('utf-8')
                        if isinstance(v, unicode):
                                v = v.encode('utf-8')
                        factoryData[k] = v
                        
                        if k == 'localqueue':
                                self.queue = v
                        if k == 'queue':
                                self.gridresource = v

                self.log.debug('_getschedconfig: Converted to: %s' % factoryData)
        except ValueError, err:
                self.log.error('_getschedconfig: %s for queue %s, downloading from %s' % (err, self.batchqueue, url))
        except IOError, (errno, errmsg):
                self.log.error('_getschedconfig: %s for queue %s, downloading from %s' % (errmsg, self.batchqueue, url))

    def getConfig(self):
        '''
        returns a Config object with the info we are interested in
        '''

        conf = Config()
        conf.add_section(self.apfqname)
        if self.gridresource:
                conf.set(self.apfqname, 'batchsubmit.gridresource', self.gridresource)
        if self.queue:
                conf.set(self.apfqname, 'batchsubmit.queue', self.queue)
  
        return conf 
