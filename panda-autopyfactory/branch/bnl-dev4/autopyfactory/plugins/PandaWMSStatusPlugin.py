#! /usr/bin/env python

from autopyfactory.factory import WMSStatusInterface
from autopyfactory.factory import Singleton 
import logging

class WMSStatus(WMSStatusInterface)

        __metaclass__ = Singleton

        def __init__(self):
                self.log = logging.getLogger("main.pandawmsstatusplugin")
                self.log.debug("PandaWMSStatusPlugin initializing...")

