import json
import time

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

    def __init__(self):

        # self.data is the structure with info reported from factories.
        # It is a dictionary of dictionaries
        # each item in first level corresponds to a factory
        # dictionary of second level has these fields:
        #   lasttime
        #   list of queues
        #
        # example:
        #
        #   self.data = {
        #           'BNL-factory1': {'time': 1439219368,
        #                        'queues': ['ANALY_BNL', 'BNL_PROD', 'ANALY_MWT2']
        #                   },
        #           'BNL-factory2': {'time': 1439219456,
        #                        'queues': ['ANALY_BNL', 'BNL_PROD', 'ANALY_MWT2', 'CLOUD_EC2']
        #                   }
        #       }

        self.data = {}
            

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

        current_time = int(time.time())
        factory = data['factory']
        queues = data['queues']
        # build the 2nd level dictionary in self.data
        factory_info = {}
        factory_info['time'] = current_time
        factory_info['queues'] = queues
        # add it to the 1st level dictionary in self.data
        self.data[factory] = factory_info
        #print 'InfoManager::add( ) -> ', self.data


    def get(self):
        """

        returns a dictionary (in JSON format):
        keys are the APFQueue names
        values are a list of factories serving that queue
        info too old is discarded
        """


        out = {}
        
        current_time = int(time.time())

        for factory, data in self.data.iteritems():
            if current_time - data['time'] > 600: # 10 minutes
                # info too old.
                pass
            else:
                for queue in data['queues']:
                    if queue in out.keys():
                        out[queue].append(factory)
                    else:
                        out[queue] = [factory]
    
        #print 'InfoManager::get( ) -> ', out

        # convert out to JSON
        out = json.dumps( out ) 
        return out

