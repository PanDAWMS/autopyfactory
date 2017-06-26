#!/usr/bin/env python

import logging
import os

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

