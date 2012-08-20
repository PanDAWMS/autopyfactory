#! /usr/bin/env python

import logging

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


class NullConfigPlugin(ConfigInterface):

    def __init__(self, apfqueue):
        self._valid = True

    def valid(self):
        return self._valid

    def getConfig(self, id):

        return Config()
