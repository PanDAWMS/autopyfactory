#!/usr/bin/env python
'''
    An X.509 proxy management component for AutoPyFactory 
'''
import logging

__author__ = "John Hover"
__copyright__ = "2010,2011, John Hover"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"


class ProxyManager(object):
    
    def __init__(self, pconfig):
        # 
        self.log = logging.getLogger('main.proxymanager')
        self.pconfig = pconfig
        self.handlers = []
        
        for sect in self.pconfig.sections():
            ph = ProxyHandler()


class ProxyHandler(threading.Thread):
    '''
    Checks, creates, and renews a VOMS proxy used by a queue. While it may seem excessive to have one per queue, 
    in order to *allow* a queue to have a unique proxy setup, it must be set for each queue. As it is all local and
    periodic, additional load should be low. 
    
    '''
    
    def __init__(self, ):
        self.baseproxy = config.get(section,'gridProxy' ) 
        self.vomsproxy = config.get(section,'vomsProxy')
        self.vorole = config.get(section, 'vorole' ) 
        self.lifetime = config.get(section, 'vomsLifetime')
        self.interval = config.get(section, 'vomsCheck')
        
        
    def run(self):
        '''
        Main thread loop. 
        '''