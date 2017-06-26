#!/usr/bin/env python

import logging
import os

from ConfigParser import ConfigParser, SafeConfigParser
from ConfigParser import NoOptionError

from autopyfactory.apfexceptions import ConfigFailure
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.interfaces import ConfigInterface

try:
     from vc3 import infoclient
except ImportError:
    pass


class VC3(ConfigInterface):
    def __init__(self, factory, config, section):

        self.log = logging.getLogger("main.configplugin")
        self.factory = factory
        self.fcl = factory.fcl
        self.log.info('ConfigPlugin: Object initialized.')
    
    def getConfig(self):
        cp = ConfigParser.ConfigParser()
        self.log.debug("Generating config object...")
        return cp
        
        
