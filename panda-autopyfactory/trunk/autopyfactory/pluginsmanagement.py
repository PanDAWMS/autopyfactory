#! /usr/bin/env python


'''
module with code for plugins management
'''

import logging
import logging.handlers
import traceback

from pprint import pprint

#from autopyfactory.configloader import Config, ConfigManager


class PluginHandler(object):

    def __init__(self):

        self.plugin_name = None
        self.plugin_class_name = None
        self.plugin_module_name = None
        self.config_section = []
        self.plugin_class = None

    def __repr__(self):
        s = ""
        s += "plugin_name = %s " % self.plugin_name
        s += "plugin_class_name = %s " % self.plugin_class_name
        s += "plugin_module_name = %s " % self.plugin_module_name
        s += "plugin_section = %s " % self.config_section
        s += "plugin_class = %s " % self.plugin_class
        return s


class QueuePluginDispatcher(object):
    '''
    class to create and deliver, on request, the different plug-ins needed for the APFQueues.
    Does not really implement any generic API, each plugin has different characteristics.
    It is just to take all the code for all APFQueue plugins in a separate class.
    '''

    def __init__(self, apfqueue):

        self.log = logging.getLogger('main.queueplugindispatcher')

        self.apfqueue = apfqueue
        self.qcl = apfqueue.qcl
        self.fcl = apfqueue.fcl
        self.mcl = apfqueue.mcl
        
        self.apfqname = apfqueue.apfqname

        self.log.debug("Getting sched plugins")
        self.schedplugins = self.getschedplugins()
        self.log.debug("Got %d sched plugins" % len(self.schedplugins))
        self.log.debug("Getting batchstatus plugin")
        self.batchstatusplugin =  self.getbatchstatusplugin()
        self.log.debug("Getting batchstatus plugin")        
        self.wmsstatusplugin =  self.getwmsstatusplugin()
        self.log.debug("Getting submit plugin")
        self.submitplugin =  self.getsubmitplugin()
        self.log.debug("Getting monitor plugins")
        self.monitorplugins = self.getmonitorplugins()
        self.log.debug("Got %d monitor plugins" % len(self.monitorplugins))

        self.log.info('QueuePluginDispatcher: Object initialized.')

    def getschedplugins(self):

        scheduler_plugin_handlers = self._getplugin('queue', 'sched')  # list of PluginHandler objects
                                                      # Note that for the Sched category,
                                                      # we allow more than one plugin 
                                                      # (split by comma in the config file)
        scheduler_plugins = []
        for scheduler_ph in scheduler_plugin_handlers:
            scheduler_cls = scheduler_ph.plugin_class
            scheduler_plugin = scheduler_cls(self.apfqueue)  # calls __init__() to instantiate the class
            scheduler_plugins.append(scheduler_plugin)
        return scheduler_plugins

    def getbatchstatusplugin(self):

        condor_q_id = 'local'
        if self.qcl.generic_get(self.apfqname, 'batchstatusplugin') == 'Condor': 
            queryargs = self.qcl.generic_get(self.apfqname, 'batchstatus.condor.queryargs')
            if queryargs:
                    condor_q_id = self.__queryargs2condorqid(queryargs)    
        batchstatus_plugin_handler = self._getplugin('queue', 'batchstatus')[0]
        batchstatus_cls = batchstatus_plugin_handler.plugin_class

        # calls __init__() to instantiate the class
        # In this case the call accepts a second arguments:
        #    an ID used to allow the creation of more than one Singleton
        #    of this category. Remember the BatchStatusPlugin class is a Singleton. 
        #    Therefore, we can have more than one
        #    Batch Status Plugin objects, each one shared by a different
        #    bunch of APF Queues.
        batchstatus_plugin = batchstatus_cls(self.apfqueue, condor_q_id=condor_q_id)  

        # starts the thread
        batchstatus_plugin.start() 
        
        return batchstatus_plugin



    ###def getwmsstatusplugin(self):
    ###
    ###    wmsstatus_plugin_handler = self._getplugin('wmsstatus')[0]
    ###    wmsstatus_cls = wmsstatus_plugin_handler.plugin_class
    ###
    ###    # calls __init__() to instantiate the class
    ###    wmsstatus_plugin = wmsstatus_cls(self.apfqueue)  
    ###
    ###    # starts the thread
    ###    wmsstatus_plugin.start()   
    ###
    ###    return wmsstatus_plugin

    def getwmsstatusplugin(self):

        condor_q_id = 'local'
        if self.qcl.generic_get(self.apfqname, 'wmsstatusplugin') == 'Condor':
            queryargs = self.qcl.generic_get(self.apfqname, 'wmsstatus.condor.queryargs')
            if queryargs:
                    condor_q_id = self.__queryargs2condorqid(queryargs)

        wmsstatus_plugin_handler = self._getplugin('queue', 'wmsstatus')[0]
        wmsstatus_cls = wmsstatus_plugin_handler.plugin_class

        # calls __init__() to instantiate the class
        if self.qcl.generic_get(self.apfqname, 'wmsstatusplugin') == 'Condor':
            # In this case the call accepts a second arguments:
            # an ID used to allow the creation of more than one Singleton
            # of this category. Remember the WMSStatusPlugin class is a Singleton. 
            # Therefore, we can have more than one
            # WMS Status Plugin objects, each one shared by a different
            # bunch of APF Queues.
            wmsstatus_plugin = wmsstatus_cls(self.apfqueue, condor_q_id=condor_q_id)
        else:
            wmsstatus_plugin = wmsstatus_cls(self.apfqueue)


        # starts the thread
        wmsstatus_plugin.start()

        return wmsstatus_plugin



    def getsubmitplugin(self):
    
        batchsubmit_plugin_handler = self._getplugin('queue', 'submit')[0]
        batchsubmit_cls = batchsubmit_plugin_handler.plugin_class
    
        # calls __init__() to instantiate the class
        batchsubmit_plugin = batchsubmit_cls(self.apfqueue)  
    
        return batchsubmit_plugin


    def getmonitorplugins(self):
        monitor_plugin_handlers = self._getplugin('queue', 'monitor', self.mcl)  # list of classes 
        self.log.debug("monitor_plugin_handlers =   %s" % monitor_plugin_handlers)
        monitor_plugins = []
        for monitor_ph in monitor_plugin_handlers:
            try:
                monitor_cls = monitor_ph.plugin_class
                monitor_id = monitor_ph.config_section[1] # the name of the section in the monitor.conf
                monitor_plugin = monitor_cls(self.apfqueue, monitor_id=monitor_id)
                monitor_plugins.append(monitor_plugin)
            except Exception, e:
                self.log.error("Problem getting monitor plugin %s" % monitor_ph.plugin_name)
                self.log.debug("Exception: %s" % traceback.format_exc())
        return monitor_plugins


    def __queryargs2condorqid(self, queryargs):
        """
        method to get the name for the condor_q singleton,
        based on the combination of the values from 
        -name and -pool input options.
        The entire list of input options come from the queues conf file,
        and it is recorded in queryargs. 
        """
        l = queryargs.split()  # convert the string into a list
                               # e.g.  ['-name', 'foo', '-pool', 'bar'....]

        name = ''
        pool = ''
        
        if '-name' in l:
            name = l[l.index('-name') + 1]
        if '-pool' in l:
            pool = l[l.index('-pool') + 1]

        if name == '' and pool == '':
            return 'local'
        else:
            return '%s:%s' %(name, pool)


    def _getplugin(self, level, action, config=None):
        '''
        Generic private method to find out the specific plugin
        to be used depending on the level and action.
        Level can be:
                - queue
                - factory
        Action can be:
                - sched
                - batchstatus
                - wmsstatus
                - batchsubmit
                - monitor

        If passed, config is an Config object, as defined in autopyfactory.configloader

        Steps taken are:
           [a] config is None:
                This means the content of the variable <action>plugin 
                in self.qcl is directly the actual plugin
 
                1. The name of the item in the config file is calculated.
                   It is supposed to have format <action>plugin.
                   For example:  schedplugin, batchstatusplugin, ...
                2. The name of the plugin module is calculated.
                   It is supposed to have format <config item><prefix>Plugin.
                   The prefix is taken from a map.
                   For example: SimpleSchedPlugin, CondorBatchStatusPlugin
                3. The plugin module is imported, using __import__
                4. The plugin class is retrieved. 
                   The name of the class is the same as the name of the module

            [b] config is not None. 
                This means the content of variable <action>section 
                points to a section in config where the actual plugin can be found.
                Therefore, there is an extra step to read the value of <action>plugin
                from the config object.

        It has been added the option of getting more than one plugins 
        of the same category. 
        The value is comma-split, and one class is retrieve for each field. 
        Then, it will be up to the invoking entity to determine if only one item
        is expected, and therefore a [0] is needed, or a list of item is possible.

        Output is a list of 2-items tuples.
        First item is the name of the plugin.
        '''

        self.log.debug("Starting for action %s" %action)


        plugin_config_item = '%splugin' %action # i.e. schedplugin
        plugin_action = action
        
        # list of objects PluginHandler
        plugin_handlers = [] 

        # Get the list of plugin names
        if config:
            config_section_item = '%ssection' % action  # i.e. monitorsection
            if self.qcl.has_option(self.apfqname, config_section_item):
                plugin_names = []
                sections = self.qcl.get(self.apfqname, config_section_item)
                for section in sections.split(','):
                    section = section.strip()
                    plugin_name = config.get(section, plugin_config_item)  # i.e. APF (from monitor.conf)
                    plugin_names.append(plugin_name)

                    ph = PluginHandler()
                    ph.plugin_name = plugin_name 
                    ph.config_section = [self.apfqname, section]
                    plugin_handlers.append(ph)
            #else:
            #    return [PluginHandler()] # temporary solution  
        
        else:
            if self.qcl.has_option(self.apfqname, plugin_config_item):
                plugin_names = self.qcl.get(self.apfqname, plugin_config_item)  # i.e. Activated
                plugin_names = plugin_names.split(',') # we convert a string split by comma into a list
               
                for plugin_name in plugin_names: 
                    if plugin_name != "None":
                        plugin_name = plugin_name.strip()
                        ph = PluginHandler()
                        ph.plugin_name = plugin_name 
                        ph.config_section = [self.apfqname]
                        plugin_handlers.append(ph)
            
            #else:
            #    return [PluginHandler()] # temporary solution  


        for ph in plugin_handlers:

            name = ph.plugin_name 

            plugin_module_name = name

            plugin_path = "autopyfactory.plugins.%s.%s.%s" % ( level, plugin_action, plugin_module_name)
            self.log.debug("Attempting to import derived classnames: %s"
                % plugin_path)

            plugin_module = __import__(plugin_path,
                                       globals(),
                                       locals(),
                                       ["%s" % plugin_module_name])

            plugin_class_name = plugin_module_name  #  the name of the class is always the name of the module
            
            self.log.debug("Attempting to return plugin with classname %s" %plugin_class_name)

            plugin_class = getattr(plugin_module, plugin_class_name)  # with getattr() we extract the actual class from the module object

            ph.plugin_class_name = plugin_class_name 
            ph.plugin_module_name = plugin_module_name 
            ph.plugin_class = plugin_class

        return plugin_handlers





####################################################
#   FIXME
#       -- Too much code duplicated between
#          FactoryPluginDispatcher and
#          QueuePluginDispatcher
#       -- things in the code that only 
#          make sense to queues plugins
#       -- the concept of "default" here
#          may be also valid for queues plugins
####################################################

class FactoryPluginDispatcher(object):
    '''
    class to create and deliver, on request, the different plug-ins needed for the Factory object.
    Does not really implement any generic API, each plugin has different characteristics.
    It is just to take all the code for all Factory plugins in a separate class.
    '''
    
    def __init__(self, factory):

        self.log = logging.getLogger('main.factoryplugindispatcher')
        self.factory = factory
        self.fcl = factory.fcl
        self.log.info('QueuePluginDispatcher: Object initialized.')


    def getconfigplugin(self):
        """
        return a Config plugin to read the APFQueues configuration.
        Typically from queues.conf or from an URL
        """

        config_plugin_handlers = self._getplugin('factory', 'config', default_plugins='File') # list of PluginHander objects, 
                                                           # as we allow more than one config plugin
        config_plugins = []
        for config_ph in config_plugin_handlers:
            config_cls = config_ph.plugin_class
            config_plugin = config_cls(self.factory)  # calls __init__() to instantiate the class
            config_plugins.append(config_plugin)
    
        return config_plugins



    def _getplugin(self, level, action, config=None, default_plugins=None):
        '''
        '''

        self.log.debug("Starting for action %s" %action)

        plugin_config_item = '%splugin' %action # i.e. schedplugin
        plugin_action = action
        
        # list of objects PluginHandler
        plugin_handlers = [] 

        # Get the list of plugin names
        if config:
            config_section_item = '%ssection' % action  # i.e. monitorsection
            if self.qcl.has_option(self.apfqname, config_section_item):
                plugin_names = []
                sections = self.qcl.get(self.apfqname, config_section_item)
                for section in sections.split(','):
                    section = section.strip()
                    plugin_name = config.get(section, plugin_config_item)  # i.e. APF (from monitor.conf)
                    plugin_names.append(plugin_name)

                    ph = PluginHandler()
                    ph.plugin_name = plugin_name 
                    ph.config_section = [self.apfqname, section]
                    plugin_handlers.append(ph)
            #else:
            #    return [PluginHandler()] # temporary solution  
        
        else:
            if self.fcl.has_option('Factory', plugin_config_item):
                plugin_names = self.fcl.get('Factory', plugin_config_item)  
                plugin_names = plugin_names.split(',') # we convert a string split by comma into a list
               
                for plugin_name in plugin_names: 
                    if plugin_name != "None":
                        plugin_name = plugin_name.strip()
                        ph = PluginHandler()
                        ph.plugin_name = plugin_name 
                        ph.config_section = ['Factory']
                        plugin_handlers.append(ph)

            # FIXME
            # too much duplicated code here
            else:
                if default_plugins:
                    plugin_names = default_plugins
                    plugin_names = plugin_names.split(',') # we convert a string split by comma into a list
               
                    for plugin_name in plugin_names: 
                        if plugin_name != "None":
                            plugin_name = plugin_name.strip()
                            ph = PluginHandler()
                            ph.plugin_name = plugin_name 
                            ph.config_section = ['Factory']
                            plugin_handlers.append(ph)

            
            #else:
            #    return [PluginHandler()] # temporary solution  


        for ph in plugin_handlers:

            name = ph.plugin_name 

            plugin_module_name = name

            plugin_path = "autopyfactory.plugins.%s.%s.%s" % ( level, plugin_action, plugin_module_name)
            self.log.debug("Attempting to import derived classnames: %s"
                % plugin_path)

            plugin_module = __import__(plugin_path,
                                       globals(),
                                       locals(),
                                       ["%s" % plugin_module_name])

            plugin_class_name = plugin_module_name  #  the name of the class is always the name of the module
            
            self.log.debug("Attempting to return plugin with classname %s" %plugin_class_name)

            plugin_class = getattr(plugin_module, plugin_class_name)  # with getattr() we extract the actual class from the module object

            ph.plugin_class_name = plugin_class_name 
            ph.plugin_module_name = plugin_module_name 
            ph.plugin_class = plugin_class

        return plugin_handlers



##############################################################################################
#           NEW CODE 
##############################################################################################

# FIXME 
log = logging.getLogger('main.pluginsdispatcher')

def getpluginclass(level, type, name):
    """
    returns the plugin class (not an object)
    """

    log.debug('starting, with values: level=%s, type=%s, name=%s' %(level, type, name))

    plugin_path = "autopyfactory.plugins.%s.%s.%s" % (level, type, name)
    plugin_module = __import__(plugin_path,
                               globals(),
                               locals(),
                               [name])
    # NOTE:
    # an alternative to __import__ would be like this
    #
    #       plugin_path = "autopyfactory/plugins/%s/%s/%s.py" % (level, type, name)
    #       import imp
    #       plugin_module = imp.load_source(name, plugin_path)
    #

    # with getattr() we extract the actual class from the module object
    # the name of the class is always the name of the module
    plugin_class = getattr(plugin_module, name)  

    log.info('returning plugin class %s' %plugin_class) 
    return plugin_class


def initializeplugin(plugin_class, *args, **kwargs):
    """
    initializes an object for a given plugin class
    and returns the object, 
    or raises an exception in case of failure
    """

    log.debug('starting, with values plugin_class=%s, *args=%s, **kwargs=%s' %(plugin_class, args, kwargs))

    try:
        plugin_object = plugin_class(*args, **kwargs)
        log.info('returning object for plugin class %s' %plugin_class)
        return plugin_object
    except:
        log.error('there was an error initializing an object for plugin class %s. Raising an exception.' %plugin_class)
        raise Exception


def getpluginnames(conf, section, type,  auxconf=None):
    """
    gets the name of the plugins to be retrieved
    from a ConfigParser object
    
    Sometimes the name of the plugin is not in the ConfigParser object.
    Instead of that, the ConfigParser objects contains the name of a section 
    in a secondary config file (auxconf) 
    """
    
    log.debug('starting, with values conf=%s, section=%s, type=%s, auxconf=%s' %(conf, section, type, auxconf))

    names = []

    if auxconf:
        auxconf_section_item = '%ssection' % type # i.e. monitorsection
        log.info('the auxiliar section name is %s' %auxconf_section_item)
        if conf.has_option(section, auxconf_section_item):
            sections = conf.get(section, auxconf_section_item)
            sections = [section.strip() for section in sections.split(',') if section.strip() != "None"]
            for section in sections:
                log.info('getting plugin names for section %s' %section)
                # recursive call to the same getpluginname() function
                # but passing the auxconf ConfigParser as primary conf 
                newnames = getpluginnames(auxconf, section, type)
                log.info('new plugin names for section %s are: %s' %(section, newnames))
                names += newnames
    
    else:
        plugin_config_item = '%splugin' %type  # i.e. schedplugin
        log.info('the section item is %s' %plugin_config_item)
        if conf.has_option(section, plugin_config_item):
            names = conf.get(section, plugin_config_item)  # i.e. Activated
            # and we convert a string split by comma into a list
            names = [name.strip() for name in names.split(',') if name.strip() != "None"]
            log.info('new plugin names are: %s' %names)

    log.info('returning list of plugin names: %s' %names)
    return names 





