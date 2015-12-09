# module to handle the info objects:
#       -- factories
#       -- queues

import logging
import time


class Factory(object):
	
    def __init__(self, factory):
        """
        self.info is a dictionary like this
            {'ANALY_BNL' : {'RUNNING':33, 'IDLE': 10, ...},
             'BNL_PROD': {'RUNNING', 22, 'IDLE': 0, ...}
            }
        """

        self.factory = factory
        self.timestamp = None
        self.queuestable = {}


    def add(self, queuetable):

        current_time = int(time.time())
        self.timestamp = current_time
	self.queuetable = queuetable

    def get(self): 




class Queue(object):

    def __init__(self, qname):
        """
        self.info is a dictionary like this:
            {'ui18': {'RUNNING':33, 'IDLE': 10, ...},
             'ui19': {'RUNNING':44, 'IDLE': 15, ...},
            }
        """

        self.qname = qname
        self.info = {}


    def add(self, factory, data):

        self.info[factory] = data

    
    def get(self):

    

