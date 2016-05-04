

"""
*** NOTE ***

    This code assumes the plugins layout is <level> / <type> / <module> 
    Like this:

        autopyfactory/
            plugins/
                queue/
                    wmsstatus/
                    monitor/
                    sched/
                        Ready.py
                        MinPerCycle.py
                        ...
                    batchstatus/
                    batchsubmit/
                factory/
                    config/ 

    For example:

        level:  queue
        type:   sched
        module: Ready

*** NOTE ***

    This code assumes that the name of the plugin classs
    is always the same than the module.
    Therefore, there is only one plugin class per module.
"""


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



