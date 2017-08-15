#! /usr/bin/env python
"""
    Configuration object loader and storage component for AutoPyFactory.
"""

import copy
import logging
import os
import traceback
import urllib2

from urllib import urlopen
from ConfigParser import SafeConfigParser, NoSectionError, InterpolationMissingOptionError

from autopyfactory.apfexceptions import ConfigFailure, ConfigFailureMandatoryAttr, FactoryConfigurationFailure


class Config(SafeConfigParser, object):
    """
    -----------------------------------------------------------------------
    Class to handle config files. 
    -----------------------------------------------------------------------
    Public Interface:
            The interface inherited from SafeConfigParser.
            merge(config, override=False)
    -----------------------------------------------------------------------
    """
    def __init__(self):

        self.log = logging.getLogger('autopyfactory')
        self.optionxform = str
        super(Config, self).__init__()

    def merge(self, config, override=False, includemissing=True):
        """
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
        """
        self.__cloneallsections(config, override, includemissing)
    
    def __cloneallsections(self, config, override, includemissing):
        """
        clone all sections in config
        """

        sections = config.sections()
        for section in sections:
            if section not in self.sections(): 
                if includemissing:
                    self.__clonesection(section, config)
            else:
                self.log.warning('section %s is duplicated. Being merged anyway.' %section)
                self.__mergesection(section, config, override, includemissing)
    
    def __clonesection(self, section, config):
        """ 
        create a new section, and copy its content
        """ 
        self.add_section(section)
        for opt in config.options(section):
            value = config.get(section, opt, raw=True)
            self.set(section, opt, value)
    
    def __mergesection(self, section, config, override, includemissing):
        """
        merge the content of a current Config object section
        with the content of the same section 
        from a different Config object
        """
        
        # determine the value of override.
        if override is not None:
            # if input option override is not None
            _override=override
        else:
            # if input option override is None
            if self.has_option(section, 'override'):
                # if the current config parser object has override...
                _override = self.getboolean(section, 'override')
            else:
                # when no one knows what to do...
                _override = False

        # NOTE:
        #   since we set default value of override=False in 
        #   method merge( ) the last else block is never executed
        #   therefore, the config variable 'override' is useless
        #   But we keep the code just in case we need it in the future

        for opt in config.options(section):
            value = config.get(section, opt, raw=True)        
            if opt not in self.options(section):
                if includemissing:
                    self.set(section, opt, value)
            else:
                if _override is False:
                    self.set(section, opt, value)

    def section2dict(self, section):
        """
        converts a given section into a dictionary
        """
        d = {}
        for opt in self.options(section):
            value = self.get(section, opt)
            d[opt] = value
        return d
    
    def clone(self):
        """
        makes an exact copy of the object
        """
        #return copy.deepcopy(self)
        # NOTE: we cannot do deepcopy because self contains 
        #       an attribute self.log = logger 
        #       that cannot be cloned

        newconfig = Config()
        newconfig.merge(self)
        return newconfig

    
    def filterkeys(self, pattern, newpattern):
        """
        it changes, for all sections, part of the name of the keys.
        For example, with inputs like 'condorgt2' and 'condorgram'
        it changes variables like 
                submit.condorgt2.environ
        for
                submit.condorgram.environ

        NOTE: we need to be careful to avoid replacing things by mistake
        So it is better to pass the longer possible patterns.
        """

        for section in self.sections():
            for key in self.options(section):
                if key.find(pattern) > -1:
                    value = self.get(section, key, raw=True)
                    newkey = key.replace(pattern, newpattern)
                    self.remove_option(section, key)
                    self.set(section, newkey, value)

        # we return self to be able to do this
        #   newconfig = config.clone().filterkeys('a', 'b')
        return self
    
    def fixpathvalues(self):
        """
        looks for values that are likely pathnames beginning with "~". 
        converts them to the full path using expanduser()
        """
        for section in self.sections():
            for key in self.options(section):
                try:
                    value = self.get(section, key, raw=True)
                    if value.startswith('~'):
                        self.set(section,key,os.path.expanduser(value))
                except InterpolationMissingOptionError, e:
                    pass

        
    def generic_get(self, section, option, get_function='get', default_value = None):      
        """
        generic get() method for Config objects.
        Inputs options are:

           section          is the  SafeConfigParser section
           option           is the option in the SafeConfigParser section
           get_function     is the string representing the actual SafeConfigParser method:  "get", "getint", "getfloat", "getboolean"
           default_value    is the default value to be returned with variable is not mandatory and is not in the config file

        example of usage:
                x = generic_get("Sec1", "x", get_function='getint', default_value=0  )
        """
        self.log.debug('called for section %s option %s get_function %s default_value %s' % ( section,
                                                                                              option,
                                                                                              get_function,
                                                                                              default_value ))                                                                                                         
        has_option = self.has_option(section, option)
        if not has_option:
            self.log.debug('option %s is not present in section %s. Return default %s' %(option, section, default_value))
            return default_value
        else:
            get_f = getattr(self, get_function)
            value = get_f(section, option)
            if value == "None" or value == "none":
                value = None
            elif value == "False" or value == "false":
                value = False
            elif value == "True" or value == "true":
                value = True
            
            self.log.debug('option %s in section %s has value %s' %(option, section, value))
            return value


    def getSection(self, section):
        """
        creates and returns a new Config object, 
        with the content of a single section
        """

        conf = Config()
        if self.has_section(section):
                conf.add_section(section)
                for item in self.items(section, raw=True):
                    conf.set(section, item[0], item[1])
        return conf

    def getContent(self, raw=True, excludelist=[]):
        """
        returns the content of the config object in a single string
        """
        str = ''
        sections = self.sections()
        sections.sort()
        for section in sections:
            str += self._getsectioncontent(section, raw, excludelist)
            str += '\n'
        return str

    def _getsectioncontent(self, section, raw, excludelist):
        """
        returns the content of a given sections in a single string
        """
        str = '[%s]\n' %section
        itemlist = self.items(section, raw=raw)
        itemlist.sort()
        for key, value in itemlist:
            if key  in excludelist:
               value = "********" 
            str += '%s = %s\n' %(key, value)
        return str


    def isequal(self, config):
        """
        this method checks if two config loader objects are equivalents:
            -- same set of sections
            -- each section have the same variables and values 
        """

        sections1 = self.sections()
        sections2 = config.sections()
        sections1.sort()
        sections2.sort()
        if sections1 != sections2:
            self.log.debug('configloader object has different list of SECTIONS than current one. Returning False') 
            return False

        # else...
        for section in self.sections():
            if not self.sectionisequal(config, section):
                self.log.debug('section %s is different in the current configloader object and the input one. Returning False' %section)
                return False
        else:
            self.log.debug('Returning True')
            return True



    def sectionisequal(self, config, section):
        """
        this method checks if a given section is equal in two configloader objects
        """

        # probably it can be done simply by 
        #   return ( self.items(section) == config.items(section) )
        # it is not done like that, yet, because I am not sure if items() would return the dictionary sorted in the same way,
        # or if that matters when comparing dictionaries
        # so, meanwhile, we just compare variable by variable

        options1 = self.options(section)
        options2 = config.options(section)
        options1.sort()
        options2.sort()
        if options1 != options2:
            self.log.debug('current configloader object and the input one has different list of options for section %s. Returning False' %section)
            return False

        # else...
        for option in self.options(section):
            if self.get(section, option) != config.get(section, option):
                self.log.debug('the value of option %s for section %s is different between the current configloader object and the input one. Returning False' %(option, section))
                return False
        else:
            self.log.debug('Returning True')
            return True
            




    def compare(self, config):
        """
        this method compares the current configloader object with a new one.
        It returns an structure saying 
            -- the list of SECTIONS that are equal,
            -- the list of SECTIONS that have changed,
            -- the list of SECTIONS that have been removed,
            -- the list of SECTIONS that have been added,
        The output is a dictionary of lists:
        
            out = {'EQUAL': ['SEC1', ..., 'SECn'],
                   'MODIFIED': ['SEC1', ..., 'SECn'],
                   'REMOVED': ['SEC1', ..., 'SECn'],
                   'ADDED': ['SEC1', ..., 'SECn'],
                  }
        """

        out = {'EQUAL': [],
               'MODIFIED': [],
               'REMOVED': [],
               'ADDED': [],
              }

        sections1 = self.sections()
        sections1.sort()
        sections2 = config.sections()
        sections2.sort()
        
        # first, we check for the SECTIONS that have been removed
        for section in sections1:
            if section not in sections2:
                out['REMOVED'].append(section)
        # it could be done in a single line like  
        # out = [section for section in sections1 if section not in sections2]

        
        # second, we check for the SECTIONS that have been added 
        for section in sections2:
            if section not in sections1:
                out['ADDED'].append(section)
        # it could be done in a single line like  
        # out = [section for section in sections2 if section not in sections1]

        # finally we search for the SECTIONS that are equal or modified
        for section in sections1:
            if section in sections2:
                if self.sectionisequal(config, section):
                    out['EQUAL'].append(section)
                else:
                    out['MODIFIED'].append(section)
        
        self.log.debug('returning with output: %s' %out) 
        return out





    def addsection(self, section, items):
        """
        method to add an entire section to a config object
        items is a dictionary with the list of key/values pairs
        """

        if section in self.sections():
            self.log.warning('section already exists. Doing nothing.')
            return
        
        self.add_section(section)
        for k,v in items.iteritems():
            if v == None:
                v = "None" # method set() only accepts strings
            self.set(section, k, v)

    


class ConfigManager(object):
    """
    -----------------------------------------------------------------------
    Class to create config files with info from different sources.
    -----------------------------------------------------------------------
    Public Interface:
            getConfig(source)
            getFromSchedConfig(site)
    -----------------------------------------------------------------------
    """

    def __init__(self):
        self.log = logging.getLogger('autopyfactory')

        ###################################
        #  NEW CODE, UNDER DEVELOPMENT    #
        ###################################
        self.sources = None
        self.configdir = None
        self.defaults = None
        ###################################
        

    def getConfig(self, sources=None, configdir=None):
        """
        creates a Config object and returns it.

        -- sources is an split by comma string, 
           where each items points to the info to feed the object:
                - path to physical file on disk
                - an URL

        -- configdir is path to a directory with a 
           set of configuration files, 
           all of them to be processed 

        """
        self.log.debug("Beginning with sources=%s and configdir=%s" % (sources,configdir))
        try:
            config = Config()
            if sources:
                for src in sources.split(','):
                    src = src.strip()
                    self.log.debug("Calling _getConfig for source %s" % src)
                    newconfig = self.__getConfig(src)
                    if newconfig:
                        config.merge(newconfig)
                        # IMPORTANT NOTE:
                        # because we create here the final configloader object
                        # by merge() of each config object (one per source)
                        # with an empty one, the 'defaults' dictionary {...} of each one
                        # is lost. 
                        # Therefore, the final configloader object has empty 'defaults' dictionary {}
            elif configdir:
                self.log.debug("Processing  configs for dir %s" % configdir)
                if os.path.isdir(configdir):
                    conffiles = [os.path.join(configdir, f) for f in os.listdir(configdir) if f.endswith('.conf')]
                    config.read(conffiles)
                    # IMPORTANT NOTE:
                    # here, as we use the native python method read()
                    # the configloader object still keeps the 'defaults' dictionary {...}
                else:
                    raise ConfigFailure('configuration directory %s does not exist' %configdir)
            config.fixpathvalues()
            self.log.debug("Finished creating config object.")
            return config
        except Exception, e:
            self.log.error("Exception: %s   %s " % ( str(e), traceback.format_exc()))
            raise ConfigFailure('creating config object from source %s failed' %sources)


    def __getConfig(self, src):
        """
        returns a new ConfigParser object 
        """
       
        data = self.__getContent(src) 
        if data:
            tmpconfig = Config()
            tmpconfig.readfp(data)
            return tmpconfig
        else:
            return None

    def __getContent(self, src):
        """
        returns the content to feed a new ConfigParser object
        """

        sourcetype = self.__getsourcetype(src)
        if sourcetype == 'file':
            if src.startswith("file://"):
                src = src[7:]
            return self.__dataFromFile(src)
        if sourcetype == 'uri':
            return self.__dataFromURI(src)

    def __getsourcetype(self, src):
        """
        determines if the source is a file on disk on an URI
        """
        self.log.debug("Determining source type for %s" % src)
        sourcetype = 'file'
        if src.startswith('file://'):
            sourcetype = 'file'
        elif src.startswith('uri://'):
            sourcetype = 'uri'
        self.log.debug("Source type is %s" % sourcetype)
        return sourcetype

    def __dataFromFile(self, path):
        """
        gets the content of an config object from  a file
        """
        try:
            path = os.path.expanduser(path)
            self.log.debug("Opening config file at %s" % path)
            f = open(path)
            return f
        except:
            raise FactoryConfigurationFailure("Problem with config file %s" % path)
    
    
    def __dataFromURI(self, uri):
        """ 
        gets the content of an config object from an URI.
        """ 
        opener = urllib2.build_opener()
        urllib2.install_opener(opener)
        try:
            uridata = urllib2.urlopen(uri)
            return uridata
        except Exception, e:
            self.log.error("Exception: %s   %s " % ( str(e), traceback.format_exc()))
            raise FactoryConfigurationFailure("Problem with URI source %s" % uri)


    ###################################
    #  NEW CODE, UNDER DEVELOPMENT    #
    ###################################

    def updateConfig(self):  # FIXME temporary name 

        if self.sources:
           config = self._updateConfigFromSources()
        if self.configdir:
           config = self._updateConfigFromDir()
        return config

    def _updateConfigFromSources(self):

        config = Config()

        if not self.defaults:

            for src in self.sources.split(','):
                src = src.strip()
                self.log.debug("Calling _getConfig for source %s" % src)
                newconfig = self.__getConfig(src)
                if newconfig:
                    config.merge(newconfig)

        else:

            tmplist = []
            for src in self.sources.split(','):
                src = src.strip()
                src = src[7:]
                tmplist.append( Config() )
                tmplist[-1].read([self.defaults, src])
            for conf in tmplist:
                config.merge(conf)

        config.fixpathvalues()
        return config


    def _updateConfigFromDir(self):

        config = Config()

        if not self.defaults:
            self.log.debug("Processing  configs for dir %s" % self.configdir)
            if os.path.isdir(self.configdir):
                conffiles = [os.path.join(self.configdir, f) for f in os.listdir(self.configdir) if f.endswith('.conf')]
                config.read(conffiles)
            else:
                raise ConfigFailure('configuration directory %s does not exist' %self.configdir)

        else:

            tmplist = []
            sources = [os.path.join(self.configdir, f) for f in os.listdir(self.configdir) if f.endswith('.conf')]
            for src in sources:
                tmplist.append( Config() )
                tmplist[-1].read([self.defaults, src])
            for conf in tmplist:
                config.merge(conf)


        config.fixpathvalues()
        return config

 

class ConfigsDiff(object):
    """
    Little class to manage the differences between 2 config loaders.
    This class can be overriden to implement specific features, 
    depending on the context and meaning of the Config objects content. 
    """

    def __init__(self, first, second):
        """
        first and second are 2 Config() objects
        """
        self.diff = first.compare(second)

    def added(self):
        return self.diff['ADDED']

    def removed(self):
        return self.diff['REMOVED']

    def modified(self):
        return self.diff['MODIFIED']

    def unmodified(self):
        return self.diff['EQUAL']


