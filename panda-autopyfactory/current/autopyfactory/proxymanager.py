#!/usr/bin/env python
'''
    An X.509 proxy management component for AutoPyFactory 


'''
import logging
import math
import os
import threading
import time

from subprocess import Popen, PIPE, STDOUT

from autopyfactory.apfexceptions import ProxyInvalidFailure


class ProxyManager(threading.Thread):
    '''
        Manager to maintain multiple ProxyHandlers, one for each target proxy. 
    
    '''
    def __init__(self, pconfig):
        threading.Thread.__init__(self) # init the thread 
        self.log = logging.getLogger('main.proxymanager')
        self.pconfig = pconfig
        self.handlers = []
        self.stopevent = threading.Event()
        for sect in self.pconfig.sections():
            ph = ProxyHandler(pconfig, sect)
            self.handlers.append(ph)
        
       
    def run(self):
        while not self.stopevent.isSet():
            try:
                self.mainLoop()    
            except Exception, e:
                self.log.error("ProxyManager mainloop threw exception: %s." % str(e))
            
            
    def mainLoop(self):
        for ph in self.handlers:
            self.log.debug("Starting handler [%s]" % ph.name)
            ph.start()
        
        try:
            while not self.stopevent.isSet():
                #self.log.debug('Checking for interrupt.')
                time.sleep(3)                  
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
        names = []
        for h in self.handlers:
            names.append(h.name)
        return names

    def getProxyPath(self, profiles):
        '''
        Check all the handlers for matching profile name(s).
        profiles argument may be a string, or a list.  
        
        '''
        for h in self.handlers:
            if h.name == name:
                return h._getProxyPath()
        return None


    def _getX509Proxy(self):
        '''
        Goes through configured proxy profiles and won't stop until either 
        1) We have gotten a valid proxy. Name of file is placed in self.x509proxy 
        2) Run out of profiles to try
        '''
        
        for profile in self.proxylist:
            try:
                self.log.debug("Getting proxy for profile %s " % profile)
                self.x509userproxy= self.getProxyPath(profile)
                self.log.info("Got valid proxy path: %s from profile [%s]" % (self.x509userproxy, profile))
                # Since we have got a good proxy, break from the profile loop
                break
            except InvalidProxyFailure, ipfe:
                self.log.warning("Got invalid proxy from profile [%s]" % profile)
        if not self.x509userproxy:
            self.log.error("Ran out of proxy profiles! No valid proxy!")
            

    def join(self,timeout=None):
            '''
            Stop the thread. Overriding this method required to handle Ctrl-C from console.
            '''
            self.log.info('Stopping all handlers...')
            for h in self.handlers:
                h.join()
            self.log.info('All handlers stopped.')            
            self.stopevent.set()
            self.log.info('Stopping thread...')
            threading.Thread.join(self, timeout)
        
class ProxyHandler(threading.Thread):
    '''
    Checks, creates, and renews a VOMS proxy. 
    or retrieves suitable credential from MyProxy 
           
    '''
    def __init__(self,config,section ):
        threading.Thread.__init__(self) # init the thread
        self.log = logging.getLogger('main.proxyhandler')
        self.name = section
        
        
        # Vars for all flavors
        self.proxyfile = os.path.expanduser(config.get(section,'proxyfile'))
        self.vorole = config.get(section, 'vorole' )         
        
        # Flavors are 'voms' or 'myproxy'
        self.flavor = config.get(section, 'flavor')
        
        if self.flavor == 'voms':        
            self.baseproxy = config.get(section,'baseproxy' ) 
            if self.baseproxy.lower().strip() == "none":
                self.baseproxy = None
            else:
                self.baseproxy = os.path.expanduser(self.baseproxy)
            
            
            self.usercert = os.path.expanduser(config.get(section, 'usercert'))
            self.userkey = os.path.expanduser(config.get(section, 'userkey'))
            
            # Handle booleans
            renewstr = config.get(section, 'renew').lower().strip()
            if renewstr == 'true':
                self.renew = True
            else:
                self.renew = False
        
        if self.flavor == 'myproxy':
            self.passphrase = None
            # Name of a proxymanager section whic will be used as the retreival credential
            self.retriever_profile = None
            self.myproxy_servername = None
            self.myproxy_
                        
            self.retriever_profile = config.get(section, 'retriever_profile')
                  
        # Handle numerics
        self.lifetime = int(config.get(section, 'lifetime'))
        self.checktime = int(config.get(section, 'checktime'))
        self.minlife = int(config.get(section, 'minlife'))
        self.interruptcheck = int(config.get(section,'interruptcheck'))

        # Handle objects
        self.stopevent = threading.Event()

        self.log.info("[%s] ProxyHandler initialized." % self.name)

    def _generateProxy(self):
        '''
        Unconditionally generates new VOMS proxy using current configuration settings for this Handler. 
        Uses existing baseproxy if configured. 
        
        '''
        self.log.debug("[%s] Generating new proxy..." % self.name)
        cmd = 'voms-proxy-init '
        #cmd += ' -dont-verify-ac '
        cmd += ' -ignorewarn '
        if self.baseproxy:
            self.log.info("[%s] Using baseproxy = %s" % (self.name, self.baseproxy))
            cmd += ' -noregen '
            cmd += ' -cert %s ' % self.baseproxy
            cmd += ' -key %s ' % self.baseproxy
        else:
            cmd += ' -cert %s ' % self.usercert
            cmd += ' -key %s ' % self.userkey
        
        cmd += ' -voms %s ' % self.vorole
        vomshours = ((self.lifetime / 60 )/ 60)
        vomshours = int(math.floor((self.lifetime / 60.0 ) / 60.0))
        if vomshours == 0:
            vomshours = 1
        cmd += ' -valid %d:00 ' % vomshours
        cmd += ' -out %s ' % self.proxyfile
             
        # Run command
        self.log.debug("[%s] Running Command: %s" % (self.name, cmd))
        p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT, close_fds=True)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            self.log.debug("[%s] Command OK. Output = %s" % (self.name, stdout))
            self.log.debug("[%s] Proxy generated successfully. Timeleft = %d" % (self.name, self._checkTimeleft()))
        elif p.returncode == 1:
            self.log.error("[%s] Command RC = 1. Error = %s" % (self.name, stderr))
        else:
            raise Exception("Strange error using command voms-proxy-init. Return code = %d" % p.returncode)


    def _retrieveMyProxyCredential(self):
        '''
        Try to retrieve valid credential from MyProxy server as configured for this handler. 
        
myproxy-init --certfile ~/.globus/cern/usercert.pem 
              --keyfile ~/.globus/cern/userkey.pem 
               -s myproxy.cern.ch 
                -Z "John Hover 241" 
                -r "John Hover 241" 
                -R "John Hover 241"


[jhover@cloudy ~]$ grid-proxy-init 
Enter GRID pass phrase for this identity:
Your identity: /DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=John Hover 241
Creating proxy ....................................................... Done
Your proxy is valid until: Thu Sep 19 02:30:59 2013
[jhover@cloudy ~]$ grid-proxy-info
subject  : /DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=John Hover 241/CN=1587635606
issuer   : /DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=John Hover 241
identity : /DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=John Hover 241
type     : RFC 3820 compliant impersonation proxy
strength : 1024 bits
path     : /tmp/x509up_u1000
timeleft : 11:59:55

myproxy-get-delegation 
  -a /tmp/x509up_u1000  
  -m atlas:/atlas/usatlas 
  -N -n 
  -o /tmp/myproxy-delegated
   
              
        '''


    def _checkTimeleft(self):
        '''
        Checks status of current proxy.         
        Returns timeleft in seconds (0 for expired or non-existent proxy)
        '''
        self.log.debug("[%s] Begin..." % self.name)
        r = 0
        if os.path.exists(self.proxyfile):
            cmd = 'voms-proxy-info -dont-verify-ac -actimeleft '
            cmd += ' -file %s ' % self.proxyfile
            
            # Run command
            self.log.debug("[%s] Running Command: %s" % (self.name, cmd))
            p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT, close_fds=True)
            stdout, stderr = p.communicate()
            if p.returncode == 0:
                self.log.debug("[%s] Command OK. Timeleft = %s" % (self.name, stdout.strip() ))
                r = int(stdout.strip())
            elif p.returncode == 1:
                self.log.warn("[%s] Command RC = 1" % self.name)
                r = 0
            else:
                raise Exception("Strange error using command voms-proxy-info -actimeleft. Return code = %d" % p.returncode)
        else:
            self.log.info('No proxy file at path %s.' % self.proxyfile)
            r = 0
        return r
    
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
                self.log.info("[%s] Running ProxyHandler..." % self.name)
                self.handleProxy()
                lastrun = int(time.time())    
            # Check relatively frequently for interrupts
            time.sleep(int(self.interruptcheck))
                          
    def handleProxy(self):
        '''
        Create proxy if timeleft is less than minimum...
        '''
        if self.renew:
            tl = self._checkTimeleft()
            self.log.debug("[%s] Time left is %d" % (self.name, tl))
            if tl < self.minlife:
                self.log.info("[%s] Need proxy. Generating..." % self.name)
                self._generateProxy()
                self.log.info("[%s] Proxy generated successfully. Timeleft = %d" % (self.name, self._checkTimeleft()))    
            else:
                self.log.debug("[%s] Time left %d seconds." % (self.name, self._checkTimeleft() ))
                self.log.info("[%s] Proxy OK (Timeleft %ds)." % ( self.name, self._checkTimeleft()))
        else:
            self.log.info("Proxy checking and renewal disabled in config.")
        
        
    def _getProxyPath(self):
        '''
        Returns file path to current, valid proxy for this Handler, e.g. /tmp/prodProxy123
        '''
        return self.proxyfile


    def _isValid(self):
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
    pm.start()
    
    
    
