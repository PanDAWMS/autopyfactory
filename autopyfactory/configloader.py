#! /usr/bin/env python
#
# $Id: configloader.py 7686 2011-04-08 21:15:43Z jhover $
#
'''
    Configuration object loader and storage component for AutoPyFactory.

'''


import logging
import os
import re
import sys

from ConfigParser import SafeConfigParser, NoSectionError
from urllib import urlopen

from autopyfactory.apfexceptions import FactoryConfigurationFailure

try:
        import json as json
except ImportError, err:
        # Not critical (yet) - try simplejson
        import simplejson as json

__author__ = "Graeme Andrew Stewart, John Hover, Jose Caballero"
__copyright__ = "2007,2008,2009,2010 Graeme Andrew Stewart; 2010,2011 John Hover; 2011 Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"


class APFConfigParser(SafeConfigParser):
    '''
    Introduces "None" -> None conversion. All else same as SafeConfigParser
    
    '''
    def get(self, section, option, *args, **kwargs):
        self.log = logging.getLogger('main.configloader')
        v = SafeConfigParser.get(self, section, option, *args, **kwargs)
        if isinstance(v, str):    
            if v.lower() == 'none':
                v = None
                self.log.debug('Detected value of "None"; converted to None.')
        return v
                
class ConfigLoader(object):
        '''
        Base class of configloader. Handles file/URI storage.
        '''
        
        def __init__(self, configFiles, loglevel=logging.DEBUG):
                self.log = logging.getLogger('main.configloader')
                if isinstance(configFiles, list):
                                self.configFiles = configFiles
                else:
                                # configFiles is a string
                                self.configFiles = [configFiles]

                self.loadConfig()
                self.configFileMtime = {}


        def loadConfig(self):
                self.config = APFConfigParser()
                # Maintain case sensitivity in keys
                self.config.optionxform = str
                self.log.info('Reading config files: %s' % self.configFiles)
                readConfigFiles = []
                unreadConfigFiles = []
                for f in self.configFiles:
                        self.log.debug('Reading file/URI: %s' % f)
                        if self._isURI(f):
                                self.log.debug('%s is a URI...' % f)
                                fp = self._convertURItoReader(f)
                                try:
                                        self.config.readfp(fp)
                                        readConfigFiles.append(f)
                                except Exception, e:
                                        self.log.debug("ERROR: %s Unable to read config file %s" % (e,f))
                                        unreadConfigFiles.append(f)
                        else:
                                self.log.debug('%s is not URI, file?:' % f)
                                try:
                                        f = os.path.expanduser(f)
                                        fp = open(f)
                                        self.config.readfp(fp)
                                        readConfigFiles.append(f)
                                except Exception, e:
                                        self.log.error("Unable to read file %s , Error: %s" % (f,e))
                                        unreadConfigFiles.append(f)
                self.log.debug('Successfully read config files %s' % readConfigFiles)
                        
                if len(unreadConfigFiles) > 0:
                        self.log.warn('Failed to read config files %s' % unreadConfigFiles)

                self._checkMandatoryValues()
                configDefaults = self._configurationDefaults()

                for section, defaultDict in configDefaults.iteritems():
                        for k, v in defaultDict.iteritems():
                                if not self.config.has_option(section, k):
                                        self.config.set(section, k, v)
                                        self.log.debug('Set default value for %s in section %s to "%s".' % (k, section, v))

        def __getattr__(self, f):
                '''
                We recover all functionalities from ConfigParser class.
                For example, now we can call from factory
                        self.qcl.get(section, item)
                or
                        self.qcl.getboolean(section, item)
                
                                
                
                '''
                return getattr(self.config, f)

        def _isURI(self, itempath):
                '''
                Tests to see if given path is a URI or filename. file:// http:// 
                (No https:// yet).           
                '''
                self.log.debug("Checking if %s is a URI..." % itempath)
                isuri = False
                itempath = itempath.strip()
                head = itempath[:7].lower()
                if head == "file://" or head == "http://":
                        isuri = True
                        self.log.debug("%s is a URI!" % itempath)
                if head == "https:/":
                        raise FactoryConfigurationFailure, "https:// URIs not supported yet."
                return isuri
                
        def _convertURItoReader(self, uri):
                '''
                Takes a URI string, opens it, and returns a filelike object of its contents.                
                '''
                self.log.debug("Converting URI %s to reader..." % uri)                
                uri = uri.strip()
                head = uri[:7].lower()
                if head == "file://": 
                        filepath = uri[7:]
                        self.log.debug("File URI detected. Opening file path %s" % filepath)
                        try:
                                reader = open(filepath)        
                        except IOError:
                                self.log.error("File path %s not openable." % filepath)
                                raise FactoryConfigurationFailure, "File URI %s does not exist or not readable" % uri  
                elif head == "http://":
                        try:
                                opener = urllib2.build_opener()
                                urllib2.install_opener( opener )
                                uridata = urllib2.urlopen( uri )
                                firstLine = uridata.readline().strip()
                                if firstLine[0] == "<":
                                        raise FactoryConfigurationFailure("First response character was '<'. Proxy error?")
                                reader = urllib2.urlopen( hostsURI )
                        except Exception:  
                                errMsg = "Couldn't find URI %s (use file://... or http://... format)" % uri
                                raise FactoryConfigurationFailure(errMsg)
                self.log.debug("Success. Returning reader." )
                return reader

        def _pythonify(self, myDict):
                '''
                Set special string values to appropriate python objects in a configuration dictionary
                '''
                for k, v in myDict.iteritems():
                        if v == 'None' or v == '':
                                myDict[k] = None
                        elif v == 'False':
                                myDict[k] = False
                        elif v == 'True':
                                myDict[k] = True
                        elif isinstance(v, str) and v.isdigit():
                                myDict[k] = int(v)

        def getConfigParser(self, section):     
                """
                creates a new SafeConfigParser object
                with the content of a given section.
                Then it can be used like this:
                        newcp = getConfigParser(section)
                        for i in newcp.items( newcp.sections()[0] ):
                                print i
                """
                newCP = SafeConfigParser()
                newCP.add_section(section)
                for item in self.config.items(section):
                        newCP.set(section, *item)
                return newCP


class FactoryConfigLoader(ConfigLoader):
        '''
        ConfigLoader for factory instance parameters.
        '''
        def loadConfig(self):
                super(FactoryConfigLoader, self).loadConfig()
                # Little bit of sanity...
                #if not os.path.isfile(self.config.get('Pilots', 'executable')):
                #        raise FactoryConfigurationFailure, 'Pilot executable %s does not seem to be a readable file.' % self.config.get('Pilots', 'executable')


        def _statConfigs(self):
                '''
                Finally, stat the conf file(s) so we can tell if they changed
                '''
                try:
                        self.configFileMtime = dict()
                        for confFile in self.configFiles:
                                self.configFileMtime[confFile] = os.stat(confFile).st_mtime
                except OSError, (errno, errMsg):
                        # This should never happen - we've just read the file after all,
                        # but belt 'n' braces...
                        raise FactoryConfigurationFailure, "Failed to stat config file %s to get modification time: %s\nDid you try to configure from a non-existent or unreadable file?" % (self.configFile, errMsg)


        def _checkMandatoryValues(self):
                '''
                Check we have a sane configuration
                '''
                mustHave = {'Factory' : ('factoryAdminEmail', 'factoryId'),
                            'Factory' : ('baseLogDir', 'baseLogDirUrl',)
                                        }
                for section in mustHave.keys():
                        if not self.config.has_section(section):
                                raise FactoryConfigurationFailure, 'Config files %s have no section [%s] (mandatory).' % (self.configFiles, section)
                        for option in mustHave[section]:
                                if not self.config.has_option(section, option):
                                        raise FactoryConfigurationFailure, 'Config files %s have no option %s in section %s (mandatory).' % (self.configFiles, option, section)

        def _configurationDefaults(self):
                '''
                Define default configuration parameters for autopyfactory instances
                '''
                defaults = {}
                try:
                        defaults = { 'Factory' : { 'condorUser' : os.environ['USER'], }}
                except KeyError:
                        # Non-login shell - you'd better set it yourself
                        defaults = { 'Factory' : { 'condorUser' : 'unknown', }}
                defaults['Factory']['schedConfigPoll'] = '5'
                return defaults


class QueueConfigLoader(ConfigLoader):
        '''
        ConfigLoader for queue-related parameters with one QueueConfigLoader per queue. 
        Since queue config sources can be URI, we have to check whether they are 'stat'-able or not. 

        This object now just handles loading queue configuration from files/URIs.
        Information from the Panda server and handling of override=True done elsewhere.  
        '''
        def loadConfig(self):
                super(QueueConfigLoader, self).loadConfig()
                # List of field names to update to their new schedconfig values (Oracle column names 
                # are case insensitive, so used lowercase here)
                deprecatedKeys = {'pilotDepth' : 'nqueue',
                                                  'pilotDepthBoost' : 'depthboost',
                                                  'idlePilotSuppression' : 'idlepilotsuppression',
                                                  'pilotLimit' : 'pilotlimit',
                                                  'transferringLimit' : 'transferringlimit',
                                                  'env': 'environ',
                                                  'jdl' : 'queue',
                                                  }

        def _checkMandatoryValues(self):
                '''
                Check we have a sane configuration
                '''
                mustHave = {
                                        #'Factory' : ('factoryAdminEmail', 'factoryId'),
                                        #'Pilots' : ('executable', 'baseLogDir', 'baseLogDirUrl',),
                                        #'QueueDefaults' : () 
                                        }
                for section in mustHave.keys():
                        if not self.config.has_section(section):
                                raise FactoryConfigurationFailure, 'Configuration files %s have no section %s (mandatory).' % (self.configFiles, section)
                        for option in mustHave[section]:
                                if not self.config.has_option(section, option):
                                        raise FactoryConfigurationFailure, 'Configuration files %s have no option %s in section %s (mandatory).' % (self.configFiles, option, section)

        def _validateQueue(self, queue):
                '''
                Perform final validation of queue configuration
                '''
                # If the queue has siteid=None it should be suppressed
                if self.queues[queue]['siteid'] == None:
                        self.configMessages.error('Queue %s has siteid=None and will be ignored. Update the queue if you really want to use it.' % queue)
                        self.queues[queue]['status'] = 'error'

        def _configurationDefaults(self):
                '''
                Defaults for the queues.conf file are handled via the standard [DEFAULTS] 
                ConfigParser approach, so we don't have to specify here. 
                '''
                
                defaults = {}
                return defaults
                  
        def _loadQueueData(self, queue):
                '''
                
                '''
                queueDataUrl = 'http://pandaserver.cern.ch:25080/cache/schedconfig/%s.factory.json' % queue
                try:
                        handle = urlopen(queueDataUrl)
                        jsonData = json.load(handle, 'utf-8')
                        handle.close()
                        self.log.debug('JSON returned: %s' % jsonData)
                        factoryData = {}
                        # json always gives back unicode strings (eh?) - convert unicode to utf-8
                        for k, v in jsonData.iteritems():
                                if isinstance(k, unicode):
                                        k = k.encode('utf-8')
                                if isinstance(v, unicode):
                                        v = v.encode('utf-8')
                                factoryData[k] = v
                        self.log.debug('Converted to: %s' % factoryData)
                except ValueError, err:
                        self.log.error('%s for queue %s, downloading from %s' % (err, queue, queueDataUrl))
                        return None
                except IOError, (errno, errmsg):
                        self.log.error('%s for queue %s, downloading from %s' % (errmsg, queue, queueDataUrl))
                        return None

                return factoryData                

