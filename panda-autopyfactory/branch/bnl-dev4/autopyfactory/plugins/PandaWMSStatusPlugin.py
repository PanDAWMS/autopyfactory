#! /usr/bin/env python

from autopyfactory.factory import WMSStatusInterface
from autopyfactory.factory import Singleton 
import logging

class Singleton(type):
     def __init__(cls, name, bases, dct):
         cls.__instance = None
         type.__init__(cls, name, bases, dct)
     def __call__(cls, *args, **kw):
         if cls.__instance is None:
             cls.__instance = type.__call__(cls, *args,**kw)
         return cls.__instance
 



        
class WMSStatus(WMSStatusInterface)

        __metaclass__ = Singleton

        def __init__(self):
                self.log = logging.getLogger("main.pandawmsstatusplugin")
                self.log.debug("PandaWMSStatusPlugin initializing...")

