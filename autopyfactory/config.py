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


class CconfigHandler(_thread):

    def __init__(self, factory):

        _thread.__init__(self)
        self.factory = factory
        self.thread_reconfig = factory.fcl.generic_get('Factory', 'thread_reconfig', 'getboolean', default_value=True):
        self.auth_reconfig = factory.fcl.generic_get('Factory', 'auth_reconfig', 'getboolean', default_value=True):


    def setconfig(self):
        # FIXME
        # ??? is this the right logic ???
        if self.thread_reconfig or self.auth_reconfig:
            self._startthread()
        else:
            # at least set configuration once
            self._run()


    def _startthread(self):
        self.factory.threadsregistry.add("core", self)
        self._thread_loop_interval = self.factory.fcl.generic_get('Factory','config.interval', 'getint', default_value=3600)
        self.start()


    def _run(self):
        # order matters here: 
        # first reconfig AuthManager, then APFQueuesManager
        self._run_auth()
        self._run_thread()


    # NOTE
    # code is duplicated for methods _run_XYZ() and getXYZConfig()
    # but for now is OK

    def _run_auth(self):
        newconfig = self.getAuthConfig()
        self.factory.authmanager.reconfig(newconfig)


    def _run_thread(self):
        newconfig = self.getQueuesConfig()
        self.factory.apfqueuesmanager.reconfig(newconfig)


    def getAuthConfig(self):
        """
        get updated configuration from the Factory Config/Auth plugins
        """
        newconfig = Config()
        for config_plugin in self.factory.auth_config_plugins:
            tmpconfig = config_plugin.getConfig()
            newconfig.merge(tmpconfig)
        return newconfig


    def getQueuesConfig(self):
        """
        get updated configuration from the Factory Config/Queues plugins
        """
        newconfig = Config()
        for config_plugin in self.factory.queues_config_plugins:
            tmpconfig = config_plugin.getConfig()
            newconfig.merge(tmpconfig)
        return newconfig
