#!/usr/bin/env python

import logging
import os

from autopyfactory.apfexceptions import ConfigFailure
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.interfaces import ConfigInterface


class Agis(ConfigInterface):
    """
    creates the configuration files with 
    information retrieved from AGIS
    """

    def __init__(self, factory):

        self.log = logging.getLogger("main.configplugin")
        self.factory = factory
        self.fcl = factory.fcl
        self.log.info('ConfigPlugin: Object initialized.')


    def getConfig(self):

        qcl = Config()


        self.log.info('queues ConfigLoader object created')
        return qcl
