#!/usr/bin/env python
'''
    An X.509 proxy management component for AutoPyFactory 
'''
import logging
import threading
import os
import time

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
            self.handlers.append(ph)
            

    def mainLoop(self):
        for ph in self.handlers:
            self.log.debug("Starting handler [%s]" % ph.name)
            ph.start()
        
        try:
            while True:
                #self.log.debug('Checking for interrupt.')
                time.sleep(1)                  
        except (KeyboardInterrupt): 
                self.log.info("Shutdown via Ctrl-C or -INT signal.")
                self.log.debug("Shutting down all threads...")
                for ph in self.handlers:
                    ph.join()
                self.log.info("All Handler threads joined. Exitting.")

    def listNames(self):
        '''
            Returns list of valid names of Handlers in this Manager. 
        '''
        


class ProxyHandler(threading.Thread):
    '''
    Checks, creates, and renews a VOMS proxy.    
    '''
    
    def __init__(self,config,section ):
        threading.Thread.__init__(self) # init the thread
        self.log = logging.getLogger('main.proxyhandler')
        self.name = section
        self.baseproxy = config.get(section,'baseproxy' ) 
        self.proxyfile = config.get(section,'proxyfile')
        self.vorole = config.get(section, 'vorole' ) 
        self.lifetime = int(config.get(section, 'lifetime'))
        self.checktime = int(config.get(section, 'checktime'))
        self.minlife = int(config.get(section, 'minlife'))
        self.interruptcheck = int(config.get(section,'interruptcheck'))
        self.usercert = os.path.expanduser(config.get(section, 'usercert'))
        self.userkey = os.path.expanduser(config.get(section, 'userkey'))
        self.stopevent = threading.Event()

    def _generateProxy(self):
        '''
        
        '''
    
    
    def join(self,timeout=None):
            '''
            Stop the thread. Overriding this method required to handle Ctrl-C from console.
            '''
            self.stopevent.set()
            self.log.info('Stopping thread...')
            threading.Thread.join(self, timeout)
   
    def run(self):
        '''
        Main thread loop. 
        '''
        # Always run the first time!
        lastrun = int(time.time()) - 10000000
        while not self.stopevent.isSet():
            now = int(time.time())
            if (now - lastrun ) < self.checktime:
                pass
            else:
                self.log.info("[%s] Running Handler cycle..." % self.name)    
            # Check relatively frequently for interrupts
            time.sleep(int(self.interruptcheck))
                          
        
    def getProxyPath(self):
        '''
        Returns file path to current, valid proxy for this Handler, e.g. /tmp/prodProxy123
        '''
        return self.proxyfile


    def validateProxy(self):
        '''
        Returns tuple (True|False , timeLeft in seconds)
        '''


  
if __name__ == '__main__':
    import getopt
    import sys
    import os
    from ConfigParser import ConfigParser
    
    debug = 0
    info = 0
    pconfig_file = None
    default_configfile = os.path.expanduser("~/etc/proxy.conf")     
    usage = """Usage: main.py [OPTIONS]  
    OPTIONS: 
        -h --help                   Print this message
        -d --debug                  Debug messages
        -v --verbose                Verbose information
        -c --config                 Config file [~/etc/proxy.conf]"""
    
    # Handle command line options
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 
                                   "c:hdv", 
                                   ["config=",
                                    "help", 
                                    "debug", 
                                    "verbose",
                                    ])
    except getopt.GetoptError, error:
        print( str(error))
        print( usage )                          
        sys.exit(1)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(usage)                     
            sys.exit()            
        elif opt in ("-c", "--config"):
            pconfig_file = arg
        elif opt in ("-d", "--debug"):
            debug = 1
        elif opt in ("-v", "--verbose"):
            info = 1

    # Set up logging. 
    # Check python version 
    major, minor, release, st, num = sys.version_info
    
    # Set up logging, handle differences between Python versions... 
    # In Python 2.3, logging.basicConfig takes no args
    #
    FORMAT23="[ %(levelname)s ] %(asctime)s %(filename)s (Line %(lineno)d): %(message)s"
    FORMAT24=FORMAT23
    FORMAT25="[%(levelname)s] %(asctime)s %(module)s.%(funcName)s(): %(message)s"
    FORMAT26=FORMAT25
    
    if major == 2:
        if minor ==3:
            formatstr = FORMAT23
        elif minor == 4:
            formatstr = FORMAT24
        elif minor == 5:
            formatstr = FORMAT25
        elif minor == 6:
            formatstr = FORMAT26
        elif minor == 7:
            formatstr = FORMAT26
    
    log = logging.getLogger()
    hdlr = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(FORMAT23)
    hdlr.setFormatter(formatter)
    log.addHandler(hdlr)
    
    if debug: 
        log.setLevel(logging.DEBUG) # Override with command line switches
    if info:
        log.setLevel(logging.INFO) # Override with command line switches
    log.info("Logging initialized.")      
    
    # Read in config file
    pconfig=ConfigParser()
    if not pconfig_file:
        pconfig_file = os.path.expanduser(default_configfile)
    else:
        pconfig_file = os.path.expanduser(pconfig_file)
    got_config = pconfig.read(pconfig_file)
    log.debug("Read config file %s, return value: %s" % (pconfig_file, got_config))
    
    pm = ProxyManager(pconfig)
    pm.mainLoop()
    
    
    