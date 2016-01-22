


def getpluginclass(level, type, name):
    """
    returns the plugin class (not an object)

    Assumes the plugins layout is <level> / <type> / <module> 
    For example:

        autopyfactory/
            plugins/
                queue/
                    wmsstatus/
                    monitor/
                    sched/
                    batchstatus/
                    batchsubmit/
                factory/
                    config/ 

    """

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

    return plugin_class


def initializeplugin(plugin_class, *k, **kw):
    """
    initializes an object for a given plugin class
    and returns the object, 
    or raises an exception in case of failure
    """

    try:
        plugin_object = plugin_class(*k, **kw)
        return plugin_object
    except:
        raise Exception


def getpluginnames(conf, section, type,  auxconf=None):
    """
    gets the name of the plugins to be retrieved
    from a ConfigParser object
    
    Sometimes the name of the plugin is not in the ConfigParser object.
    Instead of that, the ConfigParser objects contains the name of a section 
    in a secondary config file (auxconf) 
    """

    names = []

    if auxconf:
        auxconf_section_item = '%ssection' % type # i.e. monitorsection
        if conf.has_option(section, auxconf_section_item):
            sections = conf.get(section, auxconf_section_item)
            sections = [section.strip() for section in sections.split(',') if section.strip() != "None"]
            for section in sections:
                # recursive call to the same getpluginname() function
                # but passing the auxconf ConfigParser as primary conf 
                names += getpluginnames(auxconf, section, type)
    
    else:
        plugin_config_item = '%splugin' %type  # i.e. schedplugin
        if conf.has_option(section, plugin_config_item):
            names = conf.get(section, plugin_config_item)  # i.e. Activated
            # and we convert a string split by comma into a list
            names = [name.strip() for name in names.split(',') if name.strip() != "None"]

    return names 



