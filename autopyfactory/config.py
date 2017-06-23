#! /usr/bin/env python

import datetime
import logging
import logging.handlers
import threading
import time
import traceback
import os
import platform
import pwd
import smtplib
import socket
import sys

from pprint import pprint
from optparse import OptionParser
from ConfigParser import ConfigParser

try:
    from email.mime.text import MIMEText
except:
    from email.MIMEText import MIMEText


# FIXME: many of these import are not needed. They are legacy...
from autopyfactory.apfexceptions import FactoryConfigurationFailure, PandaStatusFailure, ConfigFailure
from autopyfactory.apfexceptions import CondorVersionFailure, CondorStatusFailure
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.cleanlogs import CleanLogs
from autopyfactory.logserver import LogServer
from autopyfactory.pluginmanager import PluginManager
from autopyfactory.interfaces import _thread


class Reconfig(_thread):

    def __init__(self, factory):

        _thread.__init__(self)
        factory.threadsregistry.add("core", self)
        self._thread_loop_interval = factory.fcl.generic_get('Factory','config.interval', 'getint', default_value=3600)
        self.factory = factory

    def _run(self):

        newqcl = self.getConfig()
        self.factory.apfqueuesmanager.reconfig(newqcl)


    def getConfig(self):
        """
        get updated configuration from the Factory Config plugins
        """

        newqcl = Config()
        for config_plugin in self.factory.config_plugins:
            tmpqcl = config_plugin.getConfig()
            newqcl.merge(tmpqcl)
        return newqcl


