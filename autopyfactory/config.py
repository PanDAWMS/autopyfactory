#! /usr/bin/env python

import logging
import logging.handlers


# FIXME: many of these import are not needed. They are legacy...
from autopyfactory.apfexceptions import FactoryConfigurationFailure, PandaStatusFailure, ConfigFailure
from autopyfactory.apfexceptions import CondorVersionFailure, CondorStatusFailure
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.cleanlogs import CleanLogs
from autopyfactory.logserver import LogServer
from autopyfactory.pluginmanager import PluginManager
from autopyfactory.interfaces import _thread


class Reconfig(_thread):
# FIXME !! Horrible name for a class !!

    def __init__(self, factory):

        _thread.__init__(self)
        self.factory = factory


    def setconfig(self):
        if self.factory.fcl.generic_get('Factory', 'reconfig', 'getboolean', default_value=True):
            self._startthread()
        else:
            # at least set configuration once
            self._run()


    def _startthread(self):
        self.factory.threadsregistry.add("core", self)
        self._thread_loop_interval = self.factory.fcl.generic_get('Factory','config.interval', 'getint', default_value=3600)
        self.start()


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


