import json
import time

from apps.loadbalancing.info import Factories, Queues


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

        self._logger()


    def _logger(self):

        self.log = logging.getLogger('main')
        lf = '/var/log/loadbalancing/log'
        logdir = os.path.dirname(lf)
        if not os.path.exists(logdir):
           os.makedirs(logdir)
        logStream = logging.FileHandler(filename=lf)
        FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'
        formatter = logging.Formatter(FORMAT)
        formatter.converter = time.gmtime  # to convert timestamps to UTC  
        logStream.setFormatter(formatter)
        self.log.addHandler(logStream)
        self.log.setLevel(logging.DEBUG)


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

        self.log.info('adding data: %s' %data)

        factory = data['factory']
        current_time = int(time.time())
        queues = data['queues']

        info = {}
        info['time'] = current_time
        info['queues'] = queues

        self.factories_info.add(factory, queues)
        self.queues_info.add(factory, queues)

   def get(self, data=None):
        """
        returns a dictionary (in JSON format):
        keys are the APFQueue names
        values are a list of factories serving that queue
        info too old is discarded

        data is a dictionary with parameters
        from the client
        """

        parameters = {}
        if data:
            parameters = json.loads(data)

        self.log.info('retrieving data with parameters %s' %parameters)

        out = self.queues_info.get(parameters)
        out = json.dumps(out)
        return out
