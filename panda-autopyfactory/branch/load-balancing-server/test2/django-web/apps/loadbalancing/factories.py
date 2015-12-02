import json
import time

from apps.loadbalancing.info import Factories, Queues


# FIXME : add a logger here 


class Singleton(type):
    def __init__(cls, name, bases, dct):
        cls.__instance = None
        type.__init__(cls, name, bases, dct)
    def __call__(cls, *args, **kw):
        if cls.__instance is None:
            cls.__instance = type.__call__(cls, *args,**kw)
        return cls.__instance


class InfoManager:
    #__metaclass__ = Singleton
    # when APACHE+WSGI is configured in daemon mode, this class does not need to be a Singleton

    """
    This class manages factories information.
    The information is stored into two objects:
        -- one for the factories
        -- one for the queues
    These two objects share a lot of common information,
    but having them twice makes reading it much faster in all cases.
    """

    def __init__(self):
        
        self.factories_info = Factories(self)
        self.queues_info = Queues(self)


    def add(self, data):
        """
        adds info coming from a factory to the self.data structure
        If this factory already exists in the dictionary, update also the lasttime field    
        data comes in JSON format
        data is a dictionary:
            keys are "factory" and "queues"
            the value of "queues" is a list of APFQueue names
        """

        data = json.loads(data)

        factory = data['factory']
        current_time = int(time.time())
        queues = data['queues']

        info = {}
        info['time'] = current_time
        info['queues'] = queues

        self.factories_info.add(factory, queues)
        self.queues_info.add(factory, queues)


    def get(self):
        """
        returns a dictionary (in JSON format):
        keys are the APFQueue names
        values are a list of factories serving that queue
        info too old is discarded
        """

        out = self.queues_info.get()
        out = json.dumps(out)
        return out

