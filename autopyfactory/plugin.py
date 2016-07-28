'''
  APF Module to handle plugins dispatch, config.
  Not named 'plugins' because there is a lib directory with that name.  

  Model is plugins are kept in hierarchy by 'category':
    factory: plugins that the factory uses
    queue:   plugins that APFQueues use
    profile:  plugins looked up by label
    
  Beneath the catetogory, there are types
    factory:
       config:
         file
         agis
       monitor:
         apfmon
    queue:
       wmsstatus:
         condor
         panda
       batchstatus
         condor
       batchsubmit
         condor
       sched
         <various>
    profile:
       auth:
         x509
         ssh
         cloud

Plugins initialized with ConfigParser object and a section name as arguments. The 
assumption is that the classes themselves know how to look up the info they need. 
It is also assumed that Singleton/non-Singleton decision is made during class initialization.


'''
import logging
import logging.handlers
import traceback

from pprint import pprint

class PluginManager(object):
    '''
    Entry point for plugins creation, initialization, starting, and configuration. 
    
    '''
    def __init__(self):
        '''
        Top-level object to provide plugins. 
        '''
        self.log = logging.getLogger('main.pluginmanager')
        self.log.debug('PluginManager initialized.')


    def getplugin(level, type, name, config, section):
        '''
        Provide initialized plugin object using config and section. 
        '''
        ko = self.getpluginclass(level,kind,name)
        po = ko(config=config)
        return po
        
        
    def getpluginclass(level, kind, name):
        '''
        e.g. getpluginclass('queue','monitor','APF')
        
        returns plugin class. Classes, not objects. The __init__() methods have not been 
        called yet.
        '''
        ppath = 'autopyfactory.plugins.%s.%s.%s'  %(level, kind, name)
        try:
            plugin_module = __import__(ppath, globals(), locals(), name)
        except Exception, ex:
            log.error(ex)
    
        plugin_class = getattr(plugin_module, name)
        self.log.trace("Retrieved plugin class %s" % name)
        return plugin_class
    
    def getpluginlist(level, kind, namelist, config, section):
        '''
        Provide list of initialized plugin objects. Convenience method mainly for sched 
        plugins. 
        
        '''
        plist = []
        for kn in namelist:
            kp = self.getplugin(level,kind, name, config,section)
            plist.append(kp)
        return plist








