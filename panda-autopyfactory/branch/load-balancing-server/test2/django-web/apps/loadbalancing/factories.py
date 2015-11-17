import json
import time


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
    __metaclass__ = Singleton

    """
    This class manages factories information.
    The information is stored into two objects:
        -- one for the factories
        -- one for the queues
    These two objects share a lot of common information,
    but having them twice makes reading it much faster in all cases.
    """

    def __init__(self):
        
        self.factories_info = {}
        self.queues_info = {}


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

        self.factories_info[factory] = info

        for queue in queues:
            if queue in self.queues_info.keys():
                if factory not in self.queues_info[queue]:
                    self.queues_info[queue].append(factory) 
            else:
                self.queues_info[queue] = [factory]


    def get(self):
        """
        returns a dictionary (in JSON format):
        keys are the APFQueue names
        values are a list of factories serving that queue
        info too old is discarded
        """

        ###		out = {}
        ###		
        ###		current_time = int(time.time())
        ###
        ###		for factory, data in self.data.iteritems():
        ###		    if current_time - data['time'] > 600: # 10 minutes
        ###		        # info too old.
        ###		        pass
        ###		    else:
        ###		        for queue in data['queues']:
        ###		            if queue in out.keys():
        ###		                out[queue].append(factory)
        ###		            else:
        ###		                out[queue] = [factory]
        ### 
        ###		#print 'InfoManager::get( ) -> ', out
        ###
        ###		# convert out to JSON
        ###		out = json.dumps( out ) 
        ###		return out

        ### BEGIN TEST ###
        #out = self.queues_info
        out = {}
            
        current_time = int(time.time())

        valid_factories = []
        for factory in self.factories_info.keys():
            #if current_time - self.factories_info[factory]['time'] < 600:
            if current_time - self.factories_info[factory]['time'] < 10:
               valid_factories.append(factory) 

        for queue in self.queues_info.keys():
           list_factories = []
           for factory in self.queues_info[queue]:
              if factory in valid_factories:
                 list_factories.append(factory)
           if list_factories:
              out[queue] = list_factories
        
        out = json.dumps(out)
        return out
        ### END TEST ###

