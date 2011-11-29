#! /usr/bin/env python
#
# $Id: configloader.py 7686 2011-04-08 21:15:43Z jhover $
#
'''
    Configuration object loader and storage component for AutoPyFactory.

'''


import logging

from ConfigParser import SafeConfigParser, NoSectionError
from urllib import urlopen
import urllib2

####from autopyfactory.apfexceptions import FactoryConfigurationFailure
####
####try:
####        import json as json
####except ImportError, err:
####        # Not critical (yet) - try simplejson
####        import simplejson as json

__author__ = "Graeme Andrew Stewart, John Hover, Jose Caballero"
__copyright__ = "2007,2008,2009,2010 Graeme Andrew Stewart; 2010,2011 John Hover; 2011 Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"


class Config(SafeConfigParser, object):
        '''
        -----------------------------------------------------------------------
        Class to handle config files. 
        -----------------------------------------------------------------------
        Public Interface:
                The interface inherited from SafeConfigParser.
                merge(config, override=False)
        -----------------------------------------------------------------------
        '''
        def merge(self, config, override=False):
                '''
                merge the current Config object 
                with the content of another Config object.
                '''
                self.__cloneallsections(config, override)




        def  generic_get(self, 
                         get_function,                  # string representing the actual SafeConfigParser method:  "get", "getint", "getfloat", "getboolean"
                         section,                       # SafeConfigParser section 
                         option,                        # option in the SafeConfigParser section
                         convert=False,                 # decide if string "None" should be converted into python None
                         mandatory=False,               # if the option is supposed to be there 
                         mandatory_exception=None,      # exception to be raised if the option is mandatory but it is not there 
                         log_function=None,             # log function to be used when everything goes OK
                         log_message=None,              # message to be logged when everything goes OK
                         failure_log_function,          # log function to be used when something was not OK 
                         failure_message=None ):        # message to be logged when something was not OK 
                '''
                generic get() method for Config objects.
                example of usage:
                        x = generic_get("getint", "x", convert=True, mandatory=True, mandatory_exception=NoMandatoryException, log.info, "x has a value", log.error, "x not found")
                '''
                has_option = config_object.has_option(section, option)
        
                if not has_option:
                        if mandatory:
                                if failure_log_function:
                                        failure_log_function(failure_message)
                                if mandatory_exception:
                                        raise mandatory_exception
                        else:
                                return None
                else:
                        get_f = getattr(config_object, get_function)
                        value = get_f(section, option)
                        if log_function:
                                log_function(log_message)
                        if convert:
                                if value is "None":
                                        value is None
                        return value 
        





        def __cloneallsections(self, config, override):
                '''
                clone all sections in config
                '''

                sections = config.sections()
                for section in sections:
                        if section not in self.sections(): 
                                self.__clonesection(section, config)
                        else:
                                self.__mergesection(section, config, override)

        def __clonesection(self, section, config):
                ''' 
                create a new section, and copy its content
                ''' 
                self.add_section(section)
                for opt in config.options(section):
                        value = config.get(section, opt)
                        self.set(section, opt, value)

        def __mergesection(self, section, config, override):
                '''
                merge the content of a current Config object section
                with the content of the same section 
                from a different Config object

                We loop over all options in new Config object section.

                If the option is NOT in the current object, 
                we add it with the same value.

                If the option is in the current object, 
                we override its value with the new Config object value
                if override is not True.
                '''

                for opt in config.options(section):
                        value = config.get(section, opt)        
                        if opt not in self.options(section):
                                self.set(section, opt, value)
                        else:
                                if not override:
                                        self.set(section, opt, value)



class ConfigManager:
        '''
        -----------------------------------------------------------------------
        Class to create config files with info from different sources.
        -----------------------------------------------------------------------
        Public Interface:
                getConfig(source)
                getFromSchedConfig(site)
        -----------------------------------------------------------------------
        '''

        def __init__(self):
                pass

        def getConfig(self, source):
                '''
                creates a Config object and returns it.
                source points to the info to feed the object:
                        - path to a phisical file on disk
                        - an URL
                '''

                config = Config()

                sourcetype = self.__getsourcetype(source)
                if sourcetype == 'file':
                        self.__loadfile(source, config)  # is this the best way to do it?
                if sourcetype == 'uri':
                        self.__loaduri(source, config)  # is this the best way to do it?

                return config

        def __getsourcetype(self, source):
                '''
                determines if the source is a file on disk on an URI
                '''
                sourcetype = 'file'  # default
                uritokens = ['file://', 'http://']
                for token in uritokens:
                        if source.startswith(token):
                                sourcetype = 'uri'
                                break
                return sourcetype

        def __loadfile(self, path, config):
                '''
                load a config file from disk
                '''
                f = open(path)
                config.readfp(f)

        def __loaduri(self, uri, config):
                ''' 
                load a config file from an URI
                We should first download the info into a file on disk,
                and them load that file into the Config object.
                ''' 
                opener = urllib2.build_opener()
                urllib2.install_opener(opener)
                uridata = urllib2.urlopen(uri)
                firstLine = uridata.readline().strip() 
                #if firstLine[0] == "<":
                #        raise FactoryConfigurationFailure("First response character was '<'. Proxy error?")
                reader = urllib2.urlopen(hostsURI)  #FIXME 

                config.readfp(reader) # FIXME: we should feed the Config object with info from a local file,
                                      # never directly from the URL

        def getFromSchedConfig(self, site):
                '''
                creates a Config object with info from SchedConfig
                '''

                config = Config()
                self.__querySchedConfig(site, config) # is this the best way to do it?
                return config

        def __querySchedConfig(self, site, config):
                '''
                queries SchedConfig and feed config with retrieved info.
                '''
                pass   # TO BE IMPLEMENTED

                


####################################################################################################
        
c1 = Config()
c2 = Config()

c1.readfp(open('cfg1'))
c2.readfp(open('cfg2'))

print c1.sections()
print c2.sections()
for o in c1.options('SEC12'):
        print o, c1.get('SEC12', o)
print
for o in c2.options('SEC12'):
        print o, c2.get('SEC12', o)
print
c1.merge(c2, True)
print c1.sections()
for o in c1.options('SEC12'):
        print o, c1.get('SEC12', o)


