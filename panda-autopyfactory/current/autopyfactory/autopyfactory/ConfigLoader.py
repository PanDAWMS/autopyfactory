#! /usr/bin/env python
#
#

import os, sys, logging
from ConfigParser import SafeConfigParser, NoSectionError

from autopyfactory.Exceptions import FactoryConfigurationFailure

from urllib import urlopen

try:
    import json as json
except ImportError, err:
    # Not critical (yet) - try simplejson
    import simplejson as json


class factoryConfigLoader:
    def __init__(self, factoryLogger, configFiles = ('factory.conf',), loglevel=logging.DEBUG):
        self.configMessages = logging.getLogger('main.factory.conf')
        self.configMessages.debug('Factory configLoader class initialised.')

        self.configFiles = configFiles
        self.loadConfig()


    def _pythonify(self, myDict):
        '''Set special string values to appropriate python objects in a configuration dictionary'''
        for k, v in myDict.iteritems():
            if v == 'None' or v == '':
                myDict[k] = None
            elif v == 'False':
                myDict[k] = False
            elif v == 'True':
                myDict[k] = True
            elif isinstance(v, str) and v.isdigit():
                myDict[k] = int(v)


    def _loadQueueData(self, queue):
        queueDataUrl = 'http://pandaserver.cern.ch:25080/cache/schedconfig/%s.factory.json' % queue
        try:
            handle = urlopen(queueDataUrl)
            jsonData = json.load(handle, 'utf-8')
            handle.close()
            self.configMessages.debug('JSON returned: %s' % jsonData)
            factoryData = {}
            # json always gives back unicode strings (eh?) - convert unicode to utf-8
            for k, v in jsonData.iteritems():
                if isinstance(k, unicode):
                    k = k.encode('utf-8')
                if isinstance(v, unicode):
                    v = v.encode('utf-8')
                factoryData[k] = v
            self.configMessages.debug('Converted to: %s' % factoryData)
        except ValueError, err:
            self.configMessages.warning('%s for queue %s, downloading from %s' % (err, queue, queueDataUrl))
            return None
        except IOError, (errno, errmsg):
            self.configMessages.warning('%s for queue %s, downloading from %s' % (errmsg, queue, queueDataUrl))
            return None

        return factoryData


    def _configurationDefaults(self):
        '''Define default configuration parameters for autopyfactory instances'''
        defaults = {}
        try:
            defaults = { 'Factory' : { 'condorUser' : os.environ['USER'], }}
        except KeyError:
            # Non-login shell - you'd better set it yourself
            defaults = { 'Factory' : { 'condorUser' : 'unknown', }}
        defaults['Factory']['schedConfigPoll'] = '5'

        defaults['QueueDefaults'] =  { 'status' : 'test',
                                       'nqueue' : '20',
                                       'idlepilotsuppression' : '1',
                                       'depthboost' : '2',
                                       'wallClock' : 'None',
                                       'memory' : 'None',
                                       'jobRecovery' : 'False',
                                       'pilotlimit' : 'None',
                                       'transferringlimit' : 'None',
                                       'user' : 'None',
                                       'group' : 'None',
                                       'country' : 'None',
                                       'cloud' : 'None',
                                       'server' : 'https://pandaserver.cern.ch',
                                       'jdl' : 'None',
                                       'localqueue' : 'None',
                                       'port' : '25443',
                                       'environ' : '',
                                       'proxy' : '/tmp/x509up_u%d' % os.geteuid(),
                                       'override' : 'False'
                                       }
        return defaults


    def _checkMandatoryValues(self):
        '''Check we have a sane configuration'''
        mustHave = {'Factory' : ('factoryOwner', 'factoryId'),
                    'Pilots' : ('executable', 'baseLogDir', 'baseLogDirUrl',),
                    'QueueDefaults' : () }
        for section in mustHave.keys():
            if not self.config.has_section(section):
                raise FactoryConfigurationFailure, 'Configuration files %s have no section %s (mandatory).' % (self.configFiles, section)
            for option in mustHave[section]:
                if not self.config.has_option(section, option):
                    raise FactoryConfigurationFailure, 'Configuration files %s have no option %s in section %s (mandatory).' % (self.configFiles, option, section)


    def loadConfig(self):
        self.config = SafeConfigParser()
        # Maintain case sensitivity in keys
        self.config.optionxform = str
        self.configMessages.debug('Reading configuration files %s' % self.configFiles)
        self.config.read(self.configFiles)

        self._checkMandatoryValues()
        configDefaults = self._configurationDefaults()

        for section, defaultDict in configDefaults.iteritems():
            for k, v in defaultDict.iteritems():
                if not self.config.has_option(section, k):
                    self.config.set(section, k, v)
                    self.configMessages.debug('Set default value for %s in section %s to "%s".' % (k, section, v))

        # Little bit of sanity...
        if not os.path.isfile(self.config.get('Pilots', 'executable')):
            raise FactoryConfigurationFailure, 'Pilot executable %s does not seem to be a readable file.' % self.config.get('Pilots', 'executable')

        # List of field names to update to their new schedconfig values (Oracle column names 
        # are case insensitive, so used lowercase here)
        deprecatedKeys = {'pilotDepth' : 'nqueue',
                          'pilotDepthBoost' : 'depthboost',
                          'idlePilotSuppression' : 'idlepilotsuppression',
                          'pilotLimit' : 'pilotlimit',
                          'transferringLimit' : 'transferringlimit',
                          'env': 'environ',
                          }

        # Construct the structured siteData dictionary from the configuration stanzas
        self.queues = {}
        for queue in self.config.sections():
            if queue in ('Factory', 'Pilots', 'QueueDefaults'):
                continue

            if not self.config.has_option(queue, 'nickname'):
                self.configMessages.warning('Configuration section %s has no nickname parameter - ignored.' % queue)
                continue

            # Build up basic queue information from configuration data
            self.queues[queue] = {}
            self.queues[queue]['nickname'] = self.config.get(queue, 'nickname')
            for key, value in self.config.items('QueueDefaults'):
                if self.config.has_option(queue, key):
                    self.queues[queue][key] = self.config.get(queue, key)
                    # Move away from ok, disabled status
                    if key == 'status':
                        if self.queues[queue][key] == 'ok':
                            self.configMessages.warning('Queue %s: Status "ok" is deprecated, use "online" instead.' % queue)
                            self.queues[queue][key] = 'online'
                        if self.queues[queue][key] == 'disabled':
                            self.configMessages.warning('Queue %s: Status "disabled" is deprecated, use "offline" instead.' % queue)
                            self.queues[queue][key] = 'offline'
                    # Update any deprecated keys to their new names
                    if key in deprecatedKeys.keys():
                        self.configMessages.warning('Queue %s: "%s" is deprecated, use "%s" instead.' % (queue, key, deprecatedKeys[key]))
                        self.queues[queue][deprecatedKeys[key]] = self.config.get(queue, key)
                        del self.queues[queue][key]
                else:
                    self.queues[queue][key] = self.config.get('QueueDefaults', key)
            self._pythonify(self.queues[queue])
            # Add extra information
            self.queues[queue]['pilotQueue'] = {'active' : 0, 'inactive' : 0, 'total' : 0,}
            schedConfig = self._loadQueueData(self.queues[queue]['nickname'])
            if schedConfig == None:
                if self.queues[queue]['override'] == False:
                    self.configMessages.warning('Failed to get schedconfig data for %s - ignoring this queue.' % queue)
                    del self.queues[queue]
                    continue
                else:
                    self.configMessages.warning('Failed to get schedconfig data for %s - maintaining queue because override is true.' % queue)
                        
            # Map schedConfig fields for pyfactory
            for key, value in schedConfig.iteritems():
                if self.queues[queue]['override'] and self.config.has_option(queue, key):
                    self.configMessages.warning('Queue %s has override enabled for %s, statically set to %s ignoring schedconfig value (%s).' % 
                                                (queue, key, self.queues[queue][key], value))
                    continue
                self.queues[queue][key] = value

            self.configMessages.debug("Configured queue %s as %s." % (queue, self.queues[queue]))


        # As we query panda status per site on each queue type, make a dictionary from the site back to the gatekeepers.
        # N.B. This is now split into sub dictionaries as: sites[country][group] = [site1, site2, site3, ...]
        self.sites = {}
        for queue, queueParameters in self.queues.iteritems():
            # See if we have noted a queue of this type before
            if not queueParameters['country'] in self.sites:
                self.sites[queueParameters['country']] = {}
            if not queueParameters['group'] in self.sites[queueParameters['country']]:
                self.sites[queueParameters['country']][queueParameters['group']] = {}
                self.configMessages.debug("Created new site stack group=%s, country=%s" % (queueParameters['country'], queueParameters['group']))
            if queueParameters['site'] in self.sites[queueParameters['country']][queueParameters['group']]:
                self.sites[queueParameters['country']][queueParameters['group']][queueParameters['site']].append(queue)
                self.configMessages.debug("Added queue %s from existing site %s to stack group=%s, country=%s" % \
                                               (queue, queueParameters['site'], queueParameters['country'], queueParameters['group']))
            else:
                self.sites[queueParameters['country']][queueParameters['group']][queueParameters['site']] = [queue,]
                self.configMessages.debug("Added first queue %s from site %s to new site stack group=%s, country=%s" % \
                                               (queue, queueParameters['site'], queueParameters['country'], queueParameters['group']))

        # For puny humans we have a sorted list of the queue keys so their tiny brains can find
        # the information they require ("Kill all humans!")
        self.queueKeys = self.queues.keys()
        self.queueKeys.sort()

        # Finally, stat the conf file(s) so we can tell if they changed
        try:
            self.configFileMtime = dict()
            for confFile in self.configFiles:
                self.configFileMtime[confFile] = os.stat(confFile).st_mtime
        except OSError, (errno, errMsg):
            # This should never happen - we've just read the file after all,
            # but belt 'n' braces...
            raise FactoryConfigurationFailure, "Failed to stat configuration file %s to get modifictaion time: %s\nDid you try to configure from a non-existent or unreadable file?" % (self.configFile, errMsg)

    def reloadConfigFilesIfChanged(self):
        try:
            for confFile in self.configFiles:
                if os.stat(confFile).st_mtime > self.configFileMtime[confFile]:
                    self.configMessages.info('Detected configuration file update for %s - reloading configuration' % confFile)
                    self.loadConfig()
                    break
        except OSError, (errno, errMsg):
                self.configMessages.error('Failed to stat my configuration file %s, where did you hide it? %s' % (confFile, errMsg))


    def reloadSchedConfig(self):
        '''Reload queue data from schedconfig'''
        self.configMessages.debug('Reloading schedconfig values for my queues.')
        for queue, queueParameters in self.queues.iteritems():
            schedConfig = self._loadQueueData(queueParameters['nickname'])
            if schedConfig == None:
                self.configMessages.warning('Failed to get schedconfig data for %s - leaving queue unchanged.' % queue)
                continue
            self._pythonify(schedConfig)
            for key, value in schedConfig.iteritems():
                if self.queues[queue]['override'] and self.config.has_option(queue, key):
                    self.configMessages.warning('Queue %s has override enabled for %s, statically set to %s ignoring schedconfig value (%s).' % 
                        (queue, key, self.queues[queue][key], value))
                    continue                
                if key in queueParameters and queueParameters[key] != value:
                    self.configMessages.info('New schedConfig value for %s on %s (%s)' % (key, queue, value))
                    queueParameters[key] = value
                else:
                    self.configMessages.debug('schedConfig value for %s on %s unchanged (%s)' % (key, queue, value))

                    

