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
        '''
        def __init__(self, source):
                '''
                input is a source. 
                Source can be:
                        - a physical path to a file on disk
                        - or an URI. 
                '''
                super(Config, self).__init__(self)

                self.__load(source)

        def merge(self, config, override=False):
                '''
                merge the current Config object 
                with the content of another Config object.
                '''
                self.__cloneallsections(config, override)

        def __load(self, source):
                '''
                inspects the format of the source, 
                and decides which action to perform depending on
                        - source is a path of a file
                        - source is an URI
                '''
                sourcetype = self.__gettytesource(source)
                if sourcetype == 'file':
                        self.__loadfile(source)
                if sourcetype == 'uri':
                        self.__loaduri(source)

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
                return type

        def __loadfile(self, path):
                '''
                load a config file from disk
                '''
                f = open(path)
                self.readfp(f)
                
        def __loaduri(self, uri):
                ''' 
                load a config file from an URI
                ''' 

                opener = urllib2.build_opener()
                urllib2.install_opener(opener)
                uridata = urllib2.urlopen(uri)
                firstLine = uridata.readline().strip() 
                #if firstLine[0] == "<":
                #        raise FactoryConfigurationFailure("First response character was '<'. Proxy error?")
                reader = urllib2.urlopen(hostsURI)  #FIXME
                self.readfp(reader)


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
                        value = config.get(opt)
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
                if override is True.
                '''

                for opt in config.options(section):
                        value = config.get(section, opt)        
                        if opt not in self.options(section):
                                self.set(section, opt, value)
                        else:
                                if override:
                                        self.set(section, opt, value)
                                                        



class ConfigManager:
        '''
        
        '''
        pass
