#! /usr/bin/env python
#
# $Id: exceptions.py 7652 2011-04-01 23:34:11Z jhover $
'''Exception classes for pyfactory'''



class FactoryConfigurationFailure(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class CondorStatusFailure(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class CondorVersionFailure(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class PandaStatusFailure(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class ConfigFailure(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value


class ConfigFailureMandatoryAttr(Exception):
    def __init__(self, option, section):
        self.value = 'Mandatory option %s in section %s not present.' %(option, section)
    def __str__(self):
        return self.value


class InvalidProxyFailure(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value
    
class MissingDependencyFailure(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value

class MissingPluginException(Exception):
    '''
    Exception to be raised when a plugin is being imported
    but the RPM with that plugin has not been deployed.
    '''
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value
    
class InvalidAuthFailure(Exception):
    '''
    Exception to signal when an auth profile is missing or invalid. 
    '''
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value
        
