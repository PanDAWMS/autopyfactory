#! /usr/bin/env python

class base:
    '''Base class for pilot factory backends. Fuctions do nothing, but are inherited
    by the real backends.'''
    def __init__(self):
        pass
    
    def getState(self, factory):
        pass
    
    def submitPilots(self, factory):
        pass
