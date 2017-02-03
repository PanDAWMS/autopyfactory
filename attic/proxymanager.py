#!/usr/bin/env python
'''
    An X.509 proxy management component for AutoPyFactory 

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

from subprocess import Popen, PIPE, STDOUT
from autopyfactory.apfexceptions import InvalidProxyFailure


class ProxyManager(threading.Thread):
    '''
        Manager to maintain multiple ProxyHandlers, one for each target proxy. 
    
    '''
    def __init__(self, pconfig, factory=None):
        threading.Thread.__init__(self) # init the thread 
        self.log = logging.getLogger('main.proxymanager')
        self.pconfig = pconfig
        self.factory = factory
        self.handlers = []
        self.stopevent = threading.Event()
        if factory:
            self.sleep = int(self.factory.fcl.get('Factory', 'proxymanager.sleep'))
        else:
            self.sleep = 5
        
        for sect in self.pconfig.sections():
            ph = ProxyHandler(pconfig, sect, self)
            self.handlers.append(ph)
        
       
    def run(self):
        while not self.stopevent.isSet():
            try:
                self.mainLoop()    
            except Exception, e:
                self.log.error("ProxyManager mainloop threw exception: %s." % str(e))
            
            
    def mainLoop(self):
        for ph in self.handlers:
            self.log.trace("Starting handler [%s]" % ph.name)
            ph.start()
        
        try:
            while not self.stopevent.isSet():
                #self.log.trace('Checking for interrupt.')
                time.sleep(self.sleep)                  
        except (KeyboardInterrupt): 
                self.log.debug("Shutdown via Ctrl-C or -INT signal.")
                self.log.trace("Shutting down all threads...")
                for ph in self.handlers:
                    ph.join()
                self.log.debug("All Handler threads joined. Exitting.")

    def listNames(self):
        '''
            Returns list of valid names of Handlers in this Manager. 
        '''
        names = []
        for h in self.handlers:
            names.append(h.name)
        return names

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
           

    def join(self,timeout=None):
            '''
            Stop the thread. Overriding this method required to handle Ctrl-C from console.
            '''
            self.log.debug('Stopping all handlers...')
            for h in self.handlers:
                h.join()
            self.log.debug('All handlers stopped.')            
            self.stopevent.set()
            self.log.debug('Stopping thread...')
            threading.Thread.join(self, timeout)

        
class ProxyHandler(threading.Thread):
    '''
    Checks, creates, and renews a VOMS proxy. 
    or retrieves suitable credential from MyProxy 
           
    '''
    def __init__(self,config, section, manager ):
        threading.Thread.__init__(self) # init the thread
        self.log = logging.getLogger('main.proxyhandler')
        self.name = section
        self.manager = manager
        self.owner = None
        self.group = None

        # Vars for all flavors
        self.proxyfile = os.path.expanduser(config.get(section,'proxyfile'))
        self.vorole = config.get(section, 'vorole' )         
        initdelaystr = config.get(section, 'initdelay')
        self.initdelay = int(initdelaystr)     

        # transfer proxy to remote host?
        if config.has_option(section, 'remote_host'):
            self.remote_host = config.get(section, 'remote_host')
        else:
            self.remote_host = None

        if config.has_option(section, 'remote_user'):
            self.remote_user = config.get(section, 'remote_user')
        else:
            self.remote_user = None

        if config.has_option(section, 'remote_owner'):
            self.remote_owner = config.get(section, 'remote_owner')
        else:
            self.remote_owner = None

        if config.has_option(section, 'remote_group'):
            self.remote_group = config.get(section, 'remote_group')
        else:
            self.remote_group = None

        # extra argument
        self.voms_args = None
        if config.has_option(section, 'voms.args'):
            self.voms_args = config.get(section, 'voms.args')

        
        if config.has_option(section, 'owner'):
            o = config.get(section, 'owner')
            try:
                
                p = pwd.getpwnam(o)
                self.group = grp.getgrgid(p[3])[0]
                self.owner = o
            except Exception, e:
                self.log.error("Problem getting user and group info for 'owner' = %s" % o)
        
        
        # Flavors are 'voms' or 'myproxy'
        self.flavor = config.get(section, 'flavor')
        
        if self.flavor == 'voms':
            if config.has_option(section, 'baseproxy') and\
                config.get(section, 'baseproxy').lower().strip() != "none":
                baseproxy = config.get(section,'baseproxy')
                self.baseproxy = os.path.expanduser(baseproxy)
            else:
                self.baseproxy = None
                self.usercert = os.path.expanduser(config.get(section, 'usercert'))
                self.userkey = os.path.expanduser(config.get(section, 'userkey'))

            
            # Handle booleans
            renewstr = config.get(section, 'renew').lower().strip()
            if renewstr == 'true':
                self.renew = True
            else:
                self.renew = False
        
        if self.flavor == 'myproxy':
            ''' Mandatory values:
                .vorole                atlas:/atlas/usatlas
                .myproxy_hostname      myproxy.cern.ch
                .myproxy_username
                .proxyfile             target proxy for this handler. 
        
            Optional values:
               -- Passphrase retrieval
                 .myproxy_passphrase 
           
               -- Proxy-based retrieval
                 .baseproxy
                 .retriever_list
                 
                 '''
            self.renew = True
            self.retriever_list = None
            self.myproxy_passphrase = None 
            self.myproxy_servername = None
            self.myproxy_username = None
            self.baseproxy = None
            
            self.myproxy_hostname = config.get(section,'myproxy_hostname')
            self.myproxy_username = config.get(section,'myproxy_username')
            if config.has_option(section, 'retriever_list'):
                plist = config.get(section,'retriever_list')
                # This is alist of proxy profile names specified in proxy.conf
                # We will only attempt to derive proxy file path during submission
                self.retriever_list = [x.strip() for x in plist.split(',')]
            if config.has_option(section, 'myproxy_passphrase'):
                self.myproxy_passphrase = config.get(section,'myproxy_passphrase')
                                      
        # Handle numerics
        self.lifetime = int(config.get(section, 'lifetime'))
        self.checktime = int(config.get(section, 'checktime'))
        self.minlife = int(config.get(section, 'minlife'))
        self.interruptcheck = int(config.get(section,'interruptcheck'))

        # Handle objects
        self.stopevent = threading.Event()

        self.log.debug("[%s] ProxyHandler initialized." % self.name)

    def _generateProxy(self):
        '''
        Unconditionally generates new VOMS proxy using current configuration settings for this Handler. 
        Uses existing baseproxy if configured. 
        
        '''
        self.log.trace("[%s] Generating new proxy..." % self.name)
        cmd = 'voms-proxy-init '
        #cmd += ' -dont-verify-ac '
        cmd += ' -ignorewarn '
        if self.baseproxy:
            self.log.debug("[%s] Using baseproxy = %s" % (self.name, self.baseproxy))
            cmd += ' -noregen '
            cmd += ' -cert %s ' % self.baseproxy
            cmd += ' -key %s ' % self.baseproxy
        else:
            cmd += ' -cert %s ' % self.usercert
            cmd += ' -key %s ' % self.userkey
        
        cmd += ' -voms %s ' % self.vorole
        hours = int(math.floor((self.lifetime / 60.0 ) / 60.0))
        if hours == 0:
            hours = 1
        cmd += ' -valid %d:00 ' % hours
        cmd += ' -out %s ' % self.proxyfile
        # add extra arguments if needed
        if self.voms_args:
            cmd += self.voms_args

             
        # Run command
        self.log.debug("[%s] Running Command: %s" % (self.name, cmd))
        p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT, close_fds=True)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            self.log.debug("[%s] Command OK. Output = %s" % (self.name, stdout))
            self.log.debug("[%s] Proxy generated successfully. VOMS Timeleft = %d" % (self.name, self._checkVOMSTimeLeft()))
        elif p.returncode == 1:
            self.log.error("[%s] Command RC = 1. Error = %s" % (self.name, stderr))
        else:
            raise Exception("Strange error using command voms-proxy-init. Return code = %d" % p.returncode)
        self._setProxyOwner()

        # validate the proxy
        rc = self._validateProxy() 
        return rc

    def _setProxyOwner(self):
        '''
        If owner is set, try to switch ownership of the file to the provided user and group. 
        NOTE: this only makes sense when proxymanager is run standalone by root
        '''
        if self.owner and os.access(self.proxyfile, os.F_OK):
            uid = pwd.getpwnam(self.owner).pw_uid
            gid = grp.getgrnam(self.group).gr_gid            
            try:
                os.chown(self.proxyfile, uid, gid)
                self.log.trace("Successfully set ownership for %s to %s:%s" % (self.proxyfile,
                                                                               self.owner, 
                                                                               self.group) )
            except Exception, e:
                self.log.error("Something wrong trying to do chown %s:%s %s" % (self.owner, 
                                                                                self.group, 
                                                                                self.proxyfile))
        else:
            self.log.trace("No owner requested or proxy file doesn't exist. Doing nothing.")

    def _retrieveMyProxyCredential(self):
        '''
        Try to retrieve valid credential from MyProxy server as configured for this handler. 

        Placed in MyProxy for certificate-based retrieval via:        
            myproxy-init --certfile ~/.globus/cern/usercert.pem 
                 --keyfile ~/.globus/cern/userkey.pem 
                 --username apf-user1
                 -s myproxy.cern.ch 
                 -Z "John Hover 241" 
                 -r "John Hover 241" 
                 -R "John Hover 241"
                 
        Place in MyProxy for passphrase-based retrieval via:
            myproxy-init --username apfproxy 
              --allow_anonymous_retrievers 
              --certfile ~/.globus/cern/usercert.pem 
              --keyfile ~/.globus/cern/userkey.pem 
              -s myproxy.cern.ch
                   
        '''
        self.log.trace("[%s] Begin..." % self.name)
                      
        cmd = 'myproxy-get-delegation'       
        cmd += ' --voms %s ' % self.vorole
        cmd += ' --username %s ' % self.myproxy_username
        cmd += ' --pshost %s ' % self.myproxy_hostname
        
        if self.retriever_list:
            self.baseproxy = self.manager.getProxyPath(self.retriever_list)
            cmd += ' --no_passphrase '        
            #cmd += ' --authorization %s ' % self.baseproxy
                     
        elif self.myproxy_passphrase:
            cmd += ' --stdin_pass '            
            cmd = "echo %s | %s" % ( self.myproxy_passphrase, cmd)

        hours = int(math.floor((self.lifetime / 60.0 ) / 60.0))
        if hours == 0:
            hours = 1
        cmd += ' --proxy_lifetime %d ' % hours
        cmd += ' --out %s ' % self.proxyfile
             
        # Run command
        self.log.debug("[%s] Running Command: %s" % (self.name, cmd))
        p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT, close_fds=True)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            self.log.debug("[%s] Command OK. Output = %s" % (self.name, stdout))
            self.log.debug("[%s] Proxy generated successfully. Timeleft = %d" % (self.name, self._checkVOMSTimeLeft()))
        elif p.returncode == 1:
            self.log.error("[%s] Command RC = 1. Error = %s" % (self.name, stderr))
        else:
            raise Exception("Strange error using command myproxy_get_delegation. Return code = %d" % p.returncode)
        self._setProxyOwner()
        self.log.trace("[%s] End." % self.name)


    def _checkVOMSTimeLeft(self):
        '''
        Checks status of current proxy.         
        Returns VOMS timeleft in seconds (0 for expired or non-existent proxy)
        '''
        self.log.trace("[%s] Begin..." % self.name)
        r = 0
        if os.path.exists(self.proxyfile):
            cmd = 'voms-proxy-info -dont-verify-ac -actimeleft '
            cmd += ' -file %s ' % self.proxyfile
            
            # Run command
            self.log.trace("[%s] Running Command: %s" % (self.name, cmd))
            p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT, close_fds=True)
            stdout, stderr = p.communicate()
            if p.returncode == 0:
                self.log.debug("[%s] Command OK. VOMS Timeleft = %s" % (self.name, stdout.strip() ))
                r = int(stdout.strip())
            elif p.returncode == 1:
                self.log.warn("[%s] Command RC = 1" % self.name)
                r = 0
            else:
                raise Exception("Strange error using command voms-proxy-info -actimeleft. Return code = %d" % p.returncode)
        else:
            self.log.debug('No proxy file at path %s.' % self.proxyfile)
            r = 0
        return r


    def _checkProxyTimeLeft(self):
        '''
        Checks status of current proxy.         
        Returns proxy timeleft in seconds (0 for expired or non-existent proxy)
        '''
        # FIXME: this method is almost 100% identical to _checkVOMSTimeLeft()
        #        figure out how to eliminate so much duplicate code

        self.log.trace("[%s] Begin..." % self.name)
        r = 0
        if os.path.exists(self.proxyfile):
            cmd = 'voms-proxy-info -dont-verify-ac -timeleft '
            cmd += ' -file %s ' % self.proxyfile
            
            # Run command
            self.log.debug("[%s] Running Command: %s" % (self.name, cmd))
            p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT, close_fds=True)
            stdout, stderr = p.communicate()
            if p.returncode == 0:
                self.log.debug("[%s] Command OK. Proxy Timeleft = %s" % (self.name, stdout.strip() ))
                r = int(stdout.strip())
            elif p.returncode == 1:
                self.log.warn("[%s] Command RC = 1" % self.name)
                r = 0
            else:
                raise Exception("Strange error using command voms-proxy-info -timeleft. Return code = %d" % p.returncode)
        else:
            self.log.debug('No proxy file at path %s.' % self.proxyfile)
            r = 0
        return r



    def _validateVOMS(self):
        '''
        returns the VOMS attributes of the proxy
        '''

        cmd = 'voms-proxy-info -fqan -file %s' %self.proxyfile
        # output is a list of strings like:
        #       /atlas/usatlas/Role=production/Capability=NULL
        #       /atlas/lcg1/Role=NULL/Capability=NULL
        #       /atlas/usatlas/Role=NULL/Capability=NULL
        #       /atlas/Role=NULL/Capability=NULL

        # attribute self.vorole is like 
        #       "atlas:/atlas/usatlas/Role=production"
        # so we need to get the second part
        if ':' in self.vorole:
            vorole = self.vorole.split(':')[1]
        else:
            vorole = self.vorole

        p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT, close_fds=True)
        out, err = p.communicate()
        if p.returncode == 0:
            out = out.split('\n')
            for fqan in out:
                if fqan.startswith(vorole):
                    self.log.trace('vorole %s found in proxy list of FQANs' %vorole)
                    return 0
            else:
                self.log.error('vorole %s not found in proxy' %vorole)
                return 1

        elif p.returncode == 1:
            self.log.error('command %s failed' %cmd)
            return 1


    def _validateProxy(self):
        '''
        verify the proxy generated
        is valid, has the right expiration time, VOMS attributes, etc.
        '''

        email_subject = "AutoPyFactory on %s: problem with X509 proxy" % self.manager.factory.factoryid
        
        timestamp = '%s-%s-%s %s:%s:%s (UTC)' %time.gmtime()[:6]
        host = '[%s] : ' %socket.gethostname()

        proxytimeleft = self._checkVOMSTimeLeft()
        vomstimeleft = self._checkVOMSTimeLeft()

        # check the file exists
        if not os.path.exists(self.proxyfile):
            err_msg = "proxy file %s does not exist" %self.proxyfile
            self.log.critical(err_msg)
            err_msg = timestamp + host + err_msg
            self.manager.factory.sendAdminEmail(email_subject, err_msg)
            return 1
        
        # check time of the VOMS part of a proxy
        if proxytimeleft < self.minlife:
            err_msg = "VOMS attributes for file %s has too short timeleft = %s" %(self.proxyfile, proxytimeleft)
            self.log.critical(err_msg)
            err_msg = timestamp + host + err_msg
            self.manager.factory.sendAdminEmail(email_subject, err_msg)
            return 1

        # check proxy timeleft is higher than VOMS timeleft
        proxytimeleft = self._checkProxyTimeLeft()
        if proxytimeleft < vomstimeleft:
            err_msg = "proxy timeleft (%s) is shorter than VOMS timelife (%s) for file %s" %(proxytimeleft, vomstimeleft, self.proxyfile)
            self.log.warning(err_msg)
            err_msg = timestamp + host + err_msg
            self.manager.factory.sendAdminEmail(email_subject, err_msg)

        # check VOMS attributes of the proxy
        rc = self._validateVOMS()
        if rc:
            err_msg = "proxy file %s does not have VOMS attribute %s" %(self.proxyfile, self.vorole)
            self.log.critical(err_msg)
            err_msg = timestamp + host + err_msg
            self.manager.factory.sendAdminEmail(email_subject, err_msg)
            return 1

        self.log.trace('proxy %s validated' %self.proxyfile)
        return 0



    def _transferproxy(self):
        '''
        transfer proxy to a remote host, if needed
        '''
        # TO BE IMPLEMENTED
        pass

    
    def join(self,timeout=None):
        '''
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        '''
        self.stopevent.set()
        self.log.debug('Stopping thread...')
        threading.Thread.join(self, timeout)

   
    def run(self):
        '''
        Main thread loop. 
        '''
        # Delay running to allow for other profiles to complete
        self.log.trace("Delaying %d seconds..." % self.initdelay)
        time.sleep(self.initdelay)
        
        # Always run the first time!
        lastrun = int(time.time()) - 10000000
        while not self.stopevent.isSet():
            now = int(time.time())
            if (now - lastrun ) < self.checktime:
                pass
            else:
                self.log.debug("[%s] Running ProxyHandler..." % self.name)
                self.handleProxy()
                lastrun = int(time.time())    
            # Check relatively frequently for interrupts
            time.sleep(int(self.interruptcheck))
                          
    def handleProxy(self):
        '''
        Create proxy if timeleft is less than minimum...
        '''
        if self.flavor == 'voms':
            if self.renew:
                tl = self._checkVOMSTimeLeft()
                self.log.trace("[%s] Time left is %d" % (self.name, tl))
                if tl < self.minlife:
                    self.log.debug("[%s] Need proxy. Generating..." % self.name)
                    rc = self._generateProxy()
                    if rc == 0:
                        self.log.info("[%s] Proxy generated successfully. VOMS Timeleft = %d" % (self.name, self._checkVOMSTimeLeft()))    
                    else:
                        self.log.critical("[%s] Proxy not generated successfully" % self.name)    
                else:
                    self.log.trace("[%s] VOMS Time left %d seconds." % (self.name, self._checkVOMSTimeLeft() ))
                    self.log.info("[%s] Proxy OK (VOMS Timeleft %ds)." % ( self.name, self._checkVOMSTimeLeft()))
            else:
                self.log.debug("Proxy checking and renewal disabled in config.")
        elif self.flavor == 'myproxy':
            tl = self._checkVOMSTimeLeft()
            self.log.trace("[%s] Time left is %d" % (self.name, tl))
            if tl < self.minlife:
                self.log.info("[%s] Need proxy. Retrieving..." % self.name)
                self._retrieveMyProxyCredential()
                self.log.info("[%s] Credential retrieved and proxy renewed successfully. VOMS Timeleft = %d" % (self.name, 
                                                                                                           self._checkVOMSTimeLeft()))    
            else:
                self.log.trace("[%s] VOMS Time left %d seconds." % (self.name, self._checkVOMSTimeLeft() ))
                self.log.info("[%s] Proxy OK (VOMS Timeleft %ds)." % ( self.name, self._checkVOMSTimeLeft()))

        # transfer
        self._transferproxy()
            
        
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
    from ConfigParser import ConfigParser, SafeConfigParser
    
    debug = 0
    info = 0
    pconfig_file = None
    default_configfile = os.path.expanduser("~/etc/proxy.conf")     
    usage = """Usage: proxymanager.py [OPTIONS]  
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
    pconfig=ConfigParser()
    if not pconfig_file:
        pconfig_file = os.path.expanduser(default_configfile)
    else:
        pconfig_file = os.path.expanduser(pconfig_file)
    got_config = pconfig.read(pconfig_file)
    log.trace("Read config file %s, return value: %s" % (pconfig_file, got_config))
    
    pm = ProxyManager(pconfig)
    pm.start()
    
    try:
        while True:
            time.sleep(2)
            log.trace('Checking for interrupt.')
    except (KeyboardInterrupt): 
        log.debug("Shutdown via Ctrl-C or -INT signal.")
        pm.stopevent.set()
    
    
