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


    def _updateInfo(self):

        qcl = None

        # 1. we try to read the list of files in queueConf and create a config loader
        qcf = None
        try:
            qcf = self.fcl.get('Factory', 'queueConf')    # the configuration files for queues are a list of URIs
            self.log.debug("queues.conf file(s) = %s" % qcf)
            qcl_files = ConfigManager().getConfig(sources=qcf)
            self.log.debug("successfully read config file(s) %s" % qcf)
        except:
            pass
        
        # 2. we try to read the directory in queueDirConf and create a config loader
        qcd = None
        try:
            qcd = self.fcl.get('Factory', 'queueDirConf') # the configuration files for queues are in a directory
            if qcd == "None" or qcd == "":
                qcd = None
            if qcd:
                # FIXME : temporary solution. 
                #         The ConfigManager.getConfig( ) method should know how to handle properly empty directories
                if not os.path.isdir(qcd) or os.listdir(qcd) == []:
                    self.log.warning("queues.conf directory = %s does not exist or it is empty" % qcd)
                    qcd = None
                else:
                    self.log.debug("queues.conf directory = %s" % qcd)
                    qcl_dir = ConfigManager().getConfig(configdir=qcd)
        except:
            pass
        
        # 3. we merge both loader objects
        try:
            if qcf and qcd:
                self.log.debug("both queues file(s) and dir")
                qcl = qcl_files
                qcl.merge(qcl_dir)
            elif qcf and not qcd:
                self.log.debug("queues file(s) only")
                qcl = qcl_files
            elif not qcf and qcd:
                self.log.debug("queues dir only")
                qcl = qcl_dir
            else:
                self.log.error('no files or directory with queues configuration specified')
                raise ConfigFailure('no files or directory with queues configuration specified')
        except Exception, err:
            self.log.error('Failed to create queues ConfigLoader object')
            raise ConfigFailure('Failed to create queues ConfigLoader: %s' %err)

        self.log.info('queues ConfigLoader object created')

        self.qcl = qcl


    def getConfig(self):
        self._updateInfo()
        return self.qcl
        
