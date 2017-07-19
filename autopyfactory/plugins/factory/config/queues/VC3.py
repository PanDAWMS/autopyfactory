#!/usr/bin/env python

import logging
import os

from ConfigParser import SafeConfigParser
from ConfigParser import NoOptionError

from autopyfactory.apfexceptions import ConfigFailure
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.interfaces import ConfigInterface

from vc3client import client


class VC3(ConfigInterface):
    def __init__(self, factory, config, section):

        self.log = logging.getLogger("autopyfactory.configplugin")
        self.factory = factory
        self.fcl = factory.fcl
        self.log.info('ConfigPlugin: Object initialized.')

        conf = SafeConfigParser()
	conf.readfp(open('/etc/vc3/vc3-client.conf'))
	self.clientapi = client.VC3ClientAPI(conf)
    
    def getConfig(self):
        self.log.debug("Generating config object...")
	requests = self.clientapi.listRequests()
	self.log.debug('got list of Requests = %s' %requests)
        


