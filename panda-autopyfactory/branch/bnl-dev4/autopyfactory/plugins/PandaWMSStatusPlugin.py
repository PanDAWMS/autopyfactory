#! /usr/bin/env python

import logging
import threading

from autopyfactory.factory import WMSStatusInterface
from autopyfactory.factory import Singleton 


class WMSStatus(threading.Thread, WMSStatusInterface):

        __metaclass__ = Singleton

        def __init__(self):
                self.log = logging.getLogger("main.pandawmsstatusplugin")
                self.log.debug("PandaWMSStatusPlugin initializing...")

