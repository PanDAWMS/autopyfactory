# module to handle the info objects:
#	-- factories
#	-- queues

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
    # FIXME ?? Is the Singleton really needed
    __metaclass__ = Singleton

    def __init__(self):
        self.info = {}

    def add(self, factory, queues):

        current_time = int(time.time())

        newinfo = {}
        newinfo['time'] = current_time
        newinfo['queues'] = queues

        self.info[factory] = newinfo


    def get(self):

        current_time = int(time.time())

        valid_factories = []
        for factory in self.info.keys():
            if current_time - self.factories_info[factory]['time'] < 600:
            #if current_time - self.factories_info[factory]['time'] < 10:
               valid_factories.append(factory)
		
        return valid_factories
        



class Queues:
    # FIXME ?? Is the Singleton really needed
    __metaclass__ = Singleton

    def __init__(self):
        self.info = {}

    def add(self, factory, queues):

        for queue in queues:
            if queue in self.info.keys():
                if factory not in self.info[queue]:
                    self.info[queue].append(factory)
            else:
                self.info[queue] = [factory]


    def get(self):

	queues = {}

	valid_factories = Factories().get()

        for queue in self.info.keys():
           list_factories = []
           for factory in self.info[queue]:
              if factory in valid_factories:
                 list_factories.append(factory)
           if list_factories:
              queues[queue] = list_factories

	return queues



