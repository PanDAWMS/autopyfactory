#! /usr/bin/env python
#
# $Id: configloader.py 7686 2011-04-08 21:15:43Z jhover $
#
'''
    Configuration object loader and storage component for AutoPyFactory.

'''


import copy
import logging
import os

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
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"


class ConfigException(Exception):
    def __init__(self, option, section):
        self.msg = 'option %s in section %s was supposed to be mandatory, but it is not present' %(option, section)
    def __str_(self):
        return self.msg 


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
    def merge(self, config, override=None, includemissing=True):
        '''
        merge the current Config object 
        with the content of another Config object.

        override can have only 3 values: None(default), True or False
        -- If the input override is None, 
           then the merge is done using the current override value
           that the current parser object may have. 
        -- If the input override is True, 
           then the merge is done as if the current parser object had
           override = True.
        -- If the input override is False, 
           then the merge is done as if the current parser object had
           override = False.
        When the merge is being done, values in the new parser objects
        replace the values in the current parser object, unless override=True. 
        If the current object has no override defined, 
        and the input override is None, then the default is as override=False
        (in order words, new values replace current values).

        includemissing determines if attributes in the new config parser 
        object that do not exist in the current one should be added or not.
        '''
        self.__cloneallsections(config, override, includemissing)
    
    def __cloneallsections(self, config, override, includemissing):
        '''
        clone all sections in config
        '''

        sections = config.sections()
        for section in sections:
            if section not in self.sections(): 
                if includemissing:
                    self.__clonesection(section, config)
            else:
                self.__mergesection(section, config, override, includemissing)
    
    def __clonesection(self, section, config):
        ''' 
        create a new section, and copy its content
        ''' 
        self.add_section(section)
        for opt in config.options(section):
            value = config.get(section, opt)
            self.set(section, opt, value)
    
    def __mergesection(self, section, config, override, includemissing):
        '''
        merge the content of a current Config object section
        with the content of the same section 
        from a different Config object
        '''
        
        # determine the value of override.
        if override:
            # if input option override is not None
            _override=override
        else:
            # if input option override is None
            if self.has_option(section, 'override'):
                # if the current config parser object has override...
                _override = self.get(section, 'override')
            else:
                # when no one knows what to do...
                _override = False

        for opt in config.options(section):
            value = config.get(section, opt)        
            if opt not in self.options(section):
                if includemissing:
                    self.set(section, opt, value)
            else:
                if not _override:
                    self.set(section, opt, value)
    
    def clone(self):
        '''
        makes an exact copy of the object
        '''
        return copy.deepcopy(self)
    
    def filterkeys(self, pattern, newpattern):
        '''
        it changes, for all sections, part of the name of the keys.
        For example, with inputs like 'condorgt2' and 'condorgram'
        it changes variables like 
                submit.condorgt2.environ
        for
                submit.condorgram.environ

        NOTE: we need to be careful to avoid replacing things by mistake
        So it is better to pass the longer possible patterns.
        '''

        for section in self.sections():
            for key in self.options(section):
                if key.find(pattern) > -1:
                    value = self.get(section, key)
                    newkey = key.replace(pattern, newpattern)
                    self.remove_option(section, key)
                    self.set(section, newkey, value)

        # we return self to be able to do this
        #   newconfig = config.clone().filterkeys('a', 'b')
        return self
    
    def fixpathvalues(self):
        '''
        looks for values that are likely pathnames beginning with "~". 
        converts them to the full path using expanduser()
        '''
        for section in self.sections():
            for key in self.options(section):
                value = self.get(section, key)
                if value.startswith('~'):
                    self.set(section,key,os.path.expanduser(value))
        
    def  generic_get(self, 
                     section,                       # SafeConfigParser section
                     option,                        # option in the SafeConfigParser section
                     get_function='get',            # string representing the actual SafeConfigParser method:  "get", "getint", "getfloat", "getboolean"
                     convert_to_None=False,         # decide if strings "None", "Null" or ""  should be converted into python None
                     mandatory=False,               # if the option is supposed to be there
                     default_value=None,            # default value to be returned with variable is not mandatory and is not in the config file
                     logger=None):                  # logger function 
        '''
        generic get() method for Config objects.
        example of usage:
                x = generic_get("Sec1", "x", get_function='getint', convert=True, mandatory=True, mandatory_exception=NoMandatoryException, logger=self.log)
        '''

        has_option = self.has_option(section, option)

        if not has_option:
            if mandatory:
                if logger:
                    logger.error('generic_get: option %s is not present in section %s. Will raise an exception.' %(option, section))
                raise ConfigException(option, section)
            else:
                if logger:
                    logger.info('generic_get: option %s is not present in section %s. Return default %s' %(option, section, default_value))
                return default_value
        else:
            get_f = getattr(self, get_function)
            value = get_f(section, option)
            if logger:
                logger.debug('generic_get: option %s in section %s has value %s' %(option, section, value))
            if convert_to_None:
                if value.lower() in ['none', 'null', '']:
                    value = None
            return value

    def getSection(self, section):
        '''
        creates and returns a new Config object, 
        with the content of a single section
        '''

        conf = Config()
        if self.has_section(section):
                conf.add_section(section)
                for item in self.items(section):
                    conf.set(section, item[0], item[1])
        return conf

    def getContent(self):
        '''
        returns the content of the config object in a single string
        '''
        str = ''
        for section in self.sections():
            str += self._getsectioncontent(section)
        return str

    def _getsectioncontent(self, section):
        '''
        returns the content of a given sections in a single string
        '''
        str = '[%s]\n' %section
        for item in self.items(section):
            str += '%s = %s\n' %item
        return str


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

    def getConfig(self, sources):
        '''
        creates a Config object and returns it.
        sources points to the info to feed the object:
                - path to a phisical file on disk
                - an URL
        '''
 
        config = Config()

        for src in sources.split(','):
            newconfig = self.__getConfig(src)
            if newconfig:
                    config.merge(newconfig)

        return config


    def __getConfig(self, src):
        '''
        returns a new ConfigParser object 
        '''
       
        data = self.__getContent(src) 
        if data:
            tmpconfig = Config()
            tmpconfig.readfp(data)
            tmpconfig.fixpathvalues()
            return tmpconfig
        else:
            return None

    def __getContent(self, src):
        '''
        returns the content to feed a new ConfigParser object
        '''

        sourcetype = self.__getsourcetype(src)
        if sourcetype == 'file':
            return self.__dataFromFile(src)
        if sourcetype == 'uri':
            return self.__dataFromURI(src)

    def __getsourcetype(self, src):
        '''
        determines if the source is a file on disk on an URI
        '''
        sourcetype = 'file'  # default
        uritokens = ['file://', 'http://']
        for token in uritokens:
            if src.startswith(token):
                sourcetype = 'uri'
                break
        return sourcetype

    def __dataFromFile(self, path):
        '''
        gets the content of an config object from  a file
        '''
        try:
            f = open(path)
            return f
        except:
            # most probably the file does not exist
            return None
    
    def __dataFromURI(self, uri):
        ''' 
        gets the content of an config object from an URI.
        ''' 
        opener = urllib2.build_opener()
        urllib2.install_opener(opener)
        try:
            uridata = urllib2.urlopen(uri)
            return uridata
        except:
            # most probably the URL does not exist                
            return None



    
