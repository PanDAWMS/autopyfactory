#! /usr/bin/env python

import logging
import logging.handlers
import traceback

# FIXME: many of these import are not needed. They are legacy...
from autopyfactory.apfexceptions import FactoryConfigurationFailure, PandaStatusFailure, ConfigFailure
from autopyfactory.apfexceptions import CondorVersionFailure, CondorStatusFailure
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.cleanlogs import CleanLogs
from autopyfactory.logserver import LogServer
from autopyfactory.interfaces import _thread


class ConfigHandler(_thread):

    def __init__(self, factory):

        _thread.__init__(self)
        self.log = logging.getLogger('autopyfactory.confighandler')

        self.factory = factory
        self.reconfig = factory.fcl.generic_get('Factory', 'config.reconfig', 'getboolean', default_value=False)
        self.authmanagerenabled = factory.fcl.generic_get('Factory', 'authmanager.enabled', 'getboolean', default_value=False)
        self.interval = None
        if self.reconfig:
            self.interval = factory.fcl.generic_get('Factory','config.reconfig.interval', 'getint', default_value=3600) 

        self.qcl = Config()
        self.acl = Config()


    def setconfig(self):
        self.log.debug('starting')
        # NOTE:
        # for now, we reconfig both or none
        if self.reconfig:
            self._startthread()
        else:
            # at least set configuration once
            self._run()


    def _startthread(self):
        self.log.debug('starting')
        self.factory.threadsregistry.add("core", self)
        self._thread_loop_interval = self.interval
        self.start()
        self.log.debug('leaving')


    def _run(self):
        self.log.debug('starting')
        # order matters here: 
        # first reconfig AuthManager, then APFQueuesManager
        try:
            self._run_auth()
        except Exception as e:
            self.log.error("Exception: %s   %s " % ( str(e), traceback.format_exc()))
            self.log.error('setting configuration for AuthManager failed. Will not proceed with threads configuration.')
            return
        try:
            self._run_queues()
        except Exception as e:
            self.log.error("Exception: %s   %s " % ( str(e), traceback.format_exc()))
            self.log.error('setting configuration for queues failed.')
        self.log.debug('leaving')


    def _run_auth(self):
        self.log.debug('starting')
        if self.authmanagerenabled:
            self.log.debug('auth manager is enabled. Proceeding.')
            self.acl = self.getAuthConfig()
            self.factory.authmanager.reconfig(self.acl)
            self.factory.authmanager.activate()
            self.log.debug("Completed creation of %d auth handlers." % len(self.factory.authmanager.handlers))
        self.log.debug('leaving')


    def _run_queues(self):
        self.log.debug('starting')
        self.qcl = self.getQueuesConfig()
        self.factory.apfqueuesmanager.reconfig(self.qcl)
        self.factory.apfqueuesmanager.activate() #starts all threads
        self.log.debug('leaving')


    def getAuthConfig(self):
        """
        get updated configuration from the Factory Config/Auth plugins
        """
        self.log.debug('starting')
        newconfig = Config()
        for config_plugin in self.factory.auth_config_plugins:
            tmpconfig = config_plugin.getConfig()
            newconfig.merge(tmpconfig)
        self.log.debug('leaving with newconf = %s with %s sections.' % ( newconfig, len(newconfig.sections())))
        return newconfig


    def getQueuesConfig(self):
        """
        get updated configuration from the Factory Config/Queues plugins
        """
        self.log.debug('starting')
        newconfig = Config()
        for config_plugin in self.factory.queues_config_plugins:
            tmpconfig = config_plugin.getConfig()
            newconfig.merge(tmpconfig)
        self.log.debug('leaving with newconf = %s with %s sections.' % ( newconfig, len(newconfig.sections())))
        return newconfig
