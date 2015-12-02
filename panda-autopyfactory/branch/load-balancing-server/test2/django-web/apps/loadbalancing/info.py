# module to handle the info objects:
#	-- factories
#	-- queues

import logging
import time

class Singleton(type):
    def __init__(cls, name, bases, dct):
        cls.__instance = None
        type.__init__(cls, name, bases, dct)
    def __call__(cls, *args, **kw):
        if cls.__instance is None:
            cls.__instance = type.__call__(cls, *args,**kw)
        return cls.__instance


class Factories:
    # FIXME ?? Is the Singleton really needed when APACHE+WSGI is in daemon mode
    __metaclass__ = Singleton

    def __init__(self, manager):

        self.manager = manager
        self.info = {}
        self.log = logging.getLogger('main.Factories')


    def add(self, factory, queues):

        self.log.debug('adding data. Factory: %s; queues: %s' %(factory, queues))

        current_time = int(time.time())
        newinfo = {}
        newinfo['time'] = current_time
        newinfo['queues'] = queues

        self.info[factory] = newinfo


    def get(self):

        current_time = int(time.time())

        valid_factories = []
        for factory in self.info.keys():
            if current_time - self.info[factory]['time'] < 600:
            #if current_time - self.info[factory]['time'] < 10:
               valid_factories.append(factory)
		
        self.log.info("retrieving list of factories: %s" %valid_factories)
        return valid_factories


class Queues:
    # FIXME ?? Is the Singleton really needed when APACHE+WSGI is in daemon mode
    __metaclass__ = Singleton

    def __init__(self, manager):

        self.manager = manager
        self.info = {}
        self.log = logging.getLogger('main.Queues')


    def add(self, factory, queues):

        self.log.debug('adding data. Factory: %s; queues: %s' %(factory, queues))

        for queue in queues:
            if queue in self.info.keys():
                if factory not in self.info[queue]:
                    self.info[queue].append(factory)
            else:
                self.info[queue] = [factory]


    def get(self, parameters):
        """
        parameters is a dictionary,
        passed from the client,
        with parameters for the query
        """

        maxtime = parameters.get('maxtime', 600)

        queues = {}

        valid_factories = self.manager.factories_info.get(maxtime)

        for queue in self.info.keys():
           list_factories = []
           for factory in self.info[queue]:
              if factory in valid_factories:
                 list_factories.append(factory)
           if list_factories:
              queues[queue] = list_factories

        self.log.info("retrieving queues: %s" %queues)
        return queues

