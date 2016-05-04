#! /usr/bin/env python

import logging

from urllib import urlopen

from autopyfactory.info import BaseInfo 
from autopyfactory.factory import ConfigInterface
from autopyfactory.configloader import Config, ConfigManager

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"



class URLConfigPlugin(ConfigInterface):
    '''
    -----------------------------------------------------------------------
    Class to download a Config object from an URL.
    This new Config object will be merged into some native APF Config object.
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from ConfigInterface
    -----------------------------------------------------------------------
    '''

    def __init__(self, apfqueue):
        self._valid = True

        try:

            self.apfqname = apfqueue.apfqname
            self.log = logging.getLogger("main.urlconfigplugin[%s]" %apfqueue.apfqname)
            self.log.debug("urlconfigplugin: Initializing object...")

            self.qcl = apfqueue.factory.qcl
            self.url = self.qcl.generic_get(self.apfqname, 'config.url.url', logger=self.log)

            self.log.info('urlconfigplugin: Object initialized.')
        except:
            self._valid = False

    def valid(self):
        return self._valid

    def getConfig(self):
        '''
        returns a Config object with the info we are interested in
        '''

        self.log.debug('getConfig: Leaving')

        conf_mgr = ConfigManager()
        conf = conf_mgr.getConfig(self.url)
 
        self.log.debug('getConfig: Leaving')
        return conf 

