#!/usr/bin/env python

import logging

from autopyfactory.apfexceptions import ConfigFailure
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.interfaces import ConfigInterface


class File(ConfigInterface):

    def __init__(self, factory, config, section):

        self.log = logging.getLogger('autopyfactory.config')
        self.factory = factory
        self.fcl = config
        self.qcl = None
        self.log.info('ConfigPlugin: Object initialized.')


    def getConfig(self):

        acf = self.fcl.get('Factory', 'authConf')
        self.log.debug('authmanager config file(s) = %s' %acf)
        acl = ConfigManager().getConfig(sources=acf)
        self.log.debug('successfully read config file(s) %s' %acf)
        return acl
