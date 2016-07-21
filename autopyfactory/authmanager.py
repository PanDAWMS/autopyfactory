#!/usr/bin/env python
'''
    A credential management component for AutoPyFactory 

'''
import logging
import math
import os
import pwd, grp
import threading
import time
import socket

# Added to support running module as script from arbitrary location. 
from os.path import dirname, realpath, sep, pardir
fullpathlist = realpath(__file__).split(sep)
prepath = sep.join(fullpathlist[:-2])
import sys
sys.path.insert(0, prepath)


class AuthManager(object):
    '''
        Manager to maintain multiple credential Handlers, one for each target account.
        For some handlers, if they need to perform periodic checks, they will be run
        as threads. Others, which only hold information, will just be objects.  
    
    '''
    def __init__(self, aconfig, factory=None):
        threading.Thread.__init__(self) # init the thread 
        self.log = logging.getLogger('main.authmanager')
        self.aconfig = aconfig
        self.factory = factory
        self.handlers = []

        if factory:
            self.sleep = int(self.factory.fcl.get('Factory', 'authmanager.sleep'))
        else:
            self.sleep = 5
        
        for sect in self.aconfig.sections():
            # create handler for each type of credentail in auth.conf
            
                        
            #ph = ProxyHandler(pconfig, sect, self)
            self.handlers.append(ph)
        
    def startHandlers(self):
        for ah in self.handlers:
            if isinstance(ah, threading.Thread) :
                self.log.trace("Handler [%s] is a thread. Starting..." % ah.name)
                ah.start()
            else:
                self.log.trace("Handler [%s] is not a thread. No action." % ah.name)
      
    def listNames(self):
        '''
            Returns list of valid names of Handlers in this Manager. 
        '''
        names = []
        for h in self.handlers:
            names.append(h.name)
        return names

#
#    API for X509Handler 
#
    def getProxyPath(self, profilelist):
            '''
            Check all the handlers for matching profile name(s).
            profiles argument is a list 
            '''
            pp = None
            for profile in profilelist:
                self.log.trace("Getting proxy path for profile %s" % profile)
                ph = None
                for h in self.handlers:
                    self.log.trace("Finding handler. Checking %s" % h.name)
                    if h.name == profile:
                        ph = h
                        break
                    
                if ph:  
                    self.log.trace("Found handler %s. Getting proxypath..." % ph.name)
                    pp = ph._getProxyPath()
                    self.log.trace("Proxypath is %s" % pp)
                    if pp:
                        break
            if not pp:
                subject = "Proxy problem on %s" % self.factory.factoryid
                messagestring = "Unable to get valid proxy from configured profiles: %s" % profilelist
                self.factory.sendAdminEmail(subject, messagestring)
                raise InvalidProxyFailure("Problem getting proxy for profile %s" % profilelist)
            
            return pp

#
#   API for SSHKeyHandler
#

    def getSSHKeyPair(self, profilelist):
        '''
         Returns tuple (public, private) keypair string from first valid profile in list. 
        '''
        
        pass
        
        
    def getSSHKeyPairPaths(self, profilelist):
        '''
        Returns tuple (public, private) keypair paths to files from first valid profile in 
        list. 
        '''
        pass


if __name__ == '__main__':

   
    import getopt
    import sys
    import os
    from ConfigParser import ConfigParser, SafeConfigParser
    
    debug = 0
    info = 0
    pconfig_file = None
    default_configfile = os.path.expanduser("~/etc/auth.conf")     
    usage = """Usage: authmanager.py [OPTIONS]  
    OPTIONS: 
        -h --help                   Print this message
        -d --debug                  Debug messages
        -v --verbose                Verbose information
        -c --config                 Config file [~/etc/sshcred.conf]"""
    
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
    # Add TRACE level
    logging.TRACE = 5
    logging.addLevelName(logging.TRACE, 'TRACE')
    
    def trace(self, msg, *args, **kwargs):
        self.log(logging.TRACE, msg, *args, **kwargs)
    
    logging.Logger.trace = trace
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
    log.debug("Logging initialized.")      
    
    # Read in config file
    aconfig=ConfigParser()
    if not aconfig_file:
        aconfig_file = os.path.expanduser(default_configfile)
    else:
        aconfig_file = os.path.expanduser(sconfig_file)
    got_config = aconfig.read(aconfig_file)
    log.trace("Read config file %s, return value: %s" % (aconfig_file, got_config))
    
    am = AuthManager(aconfig)
    #am.start()
    
    try:
        while True:
            time.sleep(2)
            log.trace('Checking for interrupt.')
    except (KeyboardInterrupt): 
        log.debug("Shutdown via Ctrl-C or -INT signal.")
        