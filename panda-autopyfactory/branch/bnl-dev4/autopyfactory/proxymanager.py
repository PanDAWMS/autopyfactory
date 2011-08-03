#!/usr/bin/env python
'''
    An X.509 proxy management component for AutoPyFactory 
'''
import logging
import threading

__author__ = "John Hover"
__copyright__ = "2010,2011, John Hover"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"


class ProxyManager(object):
    '''
        Manager to maintain multiple ProxyHandlers, one for each target proxy. 
    
    '''
    
    def __init__(self, pconfig):
        # 
        self.log = logging.getLogger('main.proxymanager')
        self.pconfig = pconfig
        self.handlers = []
        
        for sect in self.pconfig.sections():
            ph = ProxyHandler(pconfig, sect)
            self.handlers.add(ph)
            ph.start()

    def listNames(self):
        '''
            Returns list of valid names of Handlers in this Manager. 
        '''
        


class ProxyHandler(threading.Thread):
    '''
    Checks, creates, and renews a VOMS proxy.    
    '''
    
    def __init__(self,config,section ):
        self.log = logging.getLogger('main.proxyhandler')
        self.name = section
        self.baseproxy = config.get(section,'gridProxy' ) 
        self.vomsproxy = config.get(section,'vomsProxy')
        self.vorole = config.get(section, 'vorole' ) 
        self.lifetime = config.get(section, 'vomsLifetime')
        self.interval = config.get(section, 'vomsCheck')


    def _generateProxy(self):
        '''
        
        '''
        



        
    def run(self):
        '''
        Main thread loop. 
        '''
        
    def getProxyPath(self):
        '''
        Returns file path to current, valid proxy for this Handler, e.g. /tmp/prodProxy123
        '''

    def validateProxy(self):
        '''
        Returns tuple (True|False , timeLeft in seconds)
        '''
  
  
  
if __name__ == '__main__':
    from optparse import OptionParser
    
    
    