#!/usr/bin/env python   

#!/bin/env python
#
# General-purpose Condor glidein job wrapper, configurable by command line
#  arguments. Supports password and GSI auth. 
#
__author__ = "John Hover"
__copyright__ = "2014 John Hover"
__credits__ = []
__license__ = "GPL"
__version__ = "0.9.1"
__maintainer__ = "John Hover"
__email__ = "jhover@bnl.gov"
__status__ = "Development"


import getopt
import logging
import os
import shutil
import socket
import string
import subprocess 
import sys
import tempfile
import time
import urllib

class CondorGlidein(object):
    
    def __init__(self, condor_version="8.0.6",
                       condor_urlbase="http://dev.racf.bnl.gov/dist/condor",
                       collector="gridtest05.racf.bnl.gov",
                       port="29618",
                       auth="password", 
                       token="changeme", 
                       linger="300", 
                       startexpression="TRUE",
                       loglevel=logging.DEBUG ):

        
        self.condor_version = condor_version
        self.condor_urlbase = condor_urlbase
        self.collector = collector
        self.collector_port = port
        self.linger = linger
        self.startexpression = startexpression
        self.auth = auth
        if self.auth.lower() == 'password':
            self.password = token
        elif self.auth.lower() == 'gsi':
            self.authtok = token
            self.authlist = self.authtok.split(',')
        else:
            raise Exception("Invalid auth type: % self.auth")
                        
        try:        
            self.setup_logging(loglevel)
            self.report_args()
            self.report_info()
            self.setup_dir()
            self.set_short_hostname()
            self.handle_tarball()
            self.install_condor()
            self.configure_condor()
        except Exception, ex:
            self.log.error("Exception caught during initialization.")
            raise ex           

        self.configuration = {}
        self.configuration["COLLECTOR_HOST"] = "%s:%s" % (self.collector, self.collector_port)
        self.configuration["STARTD_NOCLAIM_SHUTDOWN"] = self.linger
        self.configuration["START"] = self.startexpression
        self.configuration["SUSPEND"] = "FALSE"
        self.configuration["PREEMPT"] = "FALSE"
        self.configuration["KILL"] = "FALSE"
        self.configuration["RANK"] = "0"
        self.configuration["CLAIM_WORKLIFE"] = "3600"
        self.configuration["JOB_RENICE_INCREMENT"] = "0"
        self.configuration["GSI_DELEGATION_KEYBITS"] = "1024"
        self.configuration["CCB_ADDRESS"] = "$(COLLECTOR_HOST)"
        self.configuration["HIGHPORT"] = "30000"
        self.configuration["LOWPORT"] = "20000"
        self.configuration["USE_SHARED_PORT"] = "TRUE"
        self.configuration["DAEMON_LIST"] = "$(DAEMON_LIST) SHARED_PORT"
        self.configuration["ALLOW_WRITE"] = "condor_pool@*"
        self.configuration["SEC_DEFAULT_AUTHENTICATION"] = "REQUIRED"
        self.configuration["SEC_DEFAULT_AUTHENTICATION_METHODS"] = "FS"
        self.configuration["SEC_ENABLE_MATCH_PASSWORD_AUTHENTICATION"] = "True"
        self.configuration["SEC_DEFAULT_ENCRYPTION"] = "REQUIRED"
        self.configuration["SEC_DEFAULT_INTEGRITY"] = "REQUIRED"
        self.configuration["ALLOW_WRITE"] = "$(ALLOW_WRITE), submit-side@matchsession/*"
        self.configuration["ALLOW_ADMINISTRATOR"] = "condor_pool@*/*"
        self.configuration["NUM_SLOTS"] = "1"
        self.configuration["SEC_DEFAULT_AUTHENTICATION_METHODS"] = "$(SEC_DEFAULT_AUTHENTICATION_METHODS), PASSWORD"
        self.configuration["SEC_PASSWORD_FILE"] = "$CONDOR_DIR/condor_password"
        self.configuration["SEC_DEFAULT_AUTHENTICATION_METHODS"] = "$(SEC_DEFAULT_AUTHENTICATION_METHODS), GSI"
        self.configuration["GSI_DAEMON_DIRECTORY"] = self.condor_dir
        self.configuration["GSI_DAEMON_TRUSTED_CA_DIR"] = "/etc/grid-security/certificates"
        self.configuration["GSI_DAEMON_PROXY"] = os.environ['X509_USER_PROXY']
        self.configuration["GSI_DAEMON_NAME"] = self.authtok
        self.configuration["GRIDMAP"] = "$(GSI_DAEMON_DIRECTORY)/grid-mapfile"



    def setup_logging(self, loglevel):
        major, minor, release, st, num = sys.version_info
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
            else:
                formatstr = FORMAT26
        self.log = logging.getLogger()
        hdlr = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(formatstr)
        hdlr.setFormatter(formatter)
        self.log.addHandler(hdlr)
        self.log.setLevel(loglevel)


    def report_info(self):
        self.log.info("Hostname: %s" % socket.gethostname())
        keys = os.environ.keys()
        keys.sort()
        envstr = ""
        for i in keys:
            envstr += " %s=%s\n" % (i,os.environ[i])
        self.log.debug("Environment:\n %s" % envstr)

    def report_args(self):
        self.log.debug("condor_version: %s" % self.condor_version)
        self.log.debug("collector: %s" % self.collector)
        self.log.debug("collector_port: %s" % self.collector_port)
        self.log.debug("auth: %s" % self.auth)
        if self.auth == "gsi":
            self.log.debug("authtok: %s" % self.authtok)
        self.log.debug("linger: %s" % self.linger)

    
    def setup_dir(self):
        self.iwd = os.getcwd()
        self.log.info("Working directory is %s" % self.iwd )
        self.condor_dir = tempfile.mkdtemp(prefix="%s/condor-glidein." % self.iwd)
        self.log.info("Condor directory is %s" % self.condor_dir )


    def handle_tarball(self):
        platform="RedHat6"
        arch="x86_64" 
        os.chdir(self.condor_dir)
        tarball_name = "condor-%s-%s_%s-stripped.tar.gz" % (condor_version, 
                                                            arch, 
                                                            platform)
        self.log.debug("tarball file is %s" % tarball_name)
        tarball_url = "%s/%s/%s/%s/%s" % (self.condor_urlbase, 
                                          self.condor_version, 
                                          platform, 
                                          arch, 
                                          tarball_name)
        self.log.info("Retrieving Condor from %s" % tarball_url)
        try:
            urllib.urlretrieve (tarball_url, tarball_name)
        
        except Exception, ex:
            self.log.error("Exception: %s" % ex)
            raise ex
        
        cmd = "file %s" % tarball_name
        out = self.runcommand(cmd)
        if "gzip compressed data" in out:
            self.log.debug("Filetype contains gzip.")
        else:
            raise Exception("File type incorrect. Failed. ")
        
        self.log.info("Download complete. File OK.")
        self.log.info("Untarring Condor...")
        cmd = "tar --verbose --extract --gzip --strip-components=1  --file=%s " % tarball_name
        self.runcommand(cmd)
        self.log.info("Untarring successful.")

    def set_short_hostname(self):
        self.log.debug("Determining short hostname...")
        shn=""
        hn = socket.gethostname()
        dotidx = hn.find('.')
        if dotidx > 0:
            shn = hn[:dotidx]
        elif dotidx < 0:
            shn = hn
        else:
            raise Exception("Problem with hostname dot location.")
        self.short_hostname = shn
        self.log.debug("Short hostname is %s" % self.short_hostname)

    def populate_gridmap(self):
        gmfpath = "%s/grid-mapfile" % self.condor_dir 
        self.log.info("Creating grid-mapfile: %s" % gmfpath)
        gms = ""
        for n in self.authlist:
            n = n.strip()
            gms += '"%s" condor_pool\n' % n
        gmf = open(gmfpath, 'w')
        gmf.write(gms)
        gmf.close()
        self.log.debug("Created grid-mapfile: %s\n%s\n" % (gmfpath, gms))

    def install_condor(self): 
        cmd = "./condor_install --type=execute"
        self.log.info("Running condor_install: '%s'" % cmd)
        os.chdir(self.condor_dir)
        self.runcommand(cmd)
        os.environ["CONDOR_CONFIG"] = "%s/etc/condor_config" % self.condor_dir
        
        self.log.info("Making config dir: %s/local.%s/config" % (self.condor_dir, 
                                                                 self.short_hostname))
        try:
            os.makedirs( "%s/local.%s/config" % (self.condor_dir, self.short_hostname))
        except OSError, oe:
            self.log.debug("Caught OS error creating local config dir. Already exists.")
        
        self.log.info("Condor installed.")    

    def configure_condor(self):
        lconfig = "%s/local.%s/condor_config.local" % (self.condor_dir, 
                                                       self.short_hostname)
        self.log.info("Local config file will be %s" % lconfig)

        cfs = ""
        cfs += "%s = %s\n" %("COLLECTOR_HOST", self.configuration["COLLECTOR_HOST"])
        cfs += "%s = %s\n" %("STARTD_NOCLAIM_SHUTDOWN", self.configuration["STARTD_NOCLAIM_SHUTDOWN"])
        cfs += "%s = %s\n" %("START", self.configuration["START"])
        cfs += "%s = %s\n" %("SUSPEND", self.configuration["SUSPEND"])
        cfs += "%s = %s\n" %("PREEMPT", self.configuration["PREEMPT"])
        cfs += "%s = %s\n" %("KILL", self.configuration["KILL"])
        cfs += "%s = %s\n" %("RANK", self.configuration["RANK"])
        cfs += "%s = %s\n" %("CLAIM_WORKLIFE", self.configuration["CLAIM_WORKLIFE"])
        cfs += "%s = %s\n" %("JOB_RENICE_INCREMENT", self.configuration["JOB_RENICE_INCREMENT"])
        cfs += "%s = %s\n" %("GSI_DELEGATION_KEYBITS", self.configuration["GSI_DELEGATION_KEYBITS"])
        cfs += "%s = %s\n" %("CCB_ADDRESS", self.configuration["CCB_ADDRESS"])
        cfs += "%s = %s\n" %("HIGHPORT", self.configuration["HIGHPORT"])
        cfs += "%s = %s\n" %("LOWPORT", self.configuration["LOWPORT"])
        cfs += "%s = %s\n" %("USE_SHARED_PORT", self.configuration["USE_SHARED_PORT"])
        cfs += "%s = %s\n" %("DAEMON_LIST", self.configuration["DAEMON_LIST"])
        cfs += "%s = %s\n" %("ALLOW_WRITE", self.configuration["ALLOW_WRITE"])
        cfs += "%s = %s\n" %("SEC_DEFAULT_AUTHENTICATION", self.configuration["SEC_DEFAULT_AUTHENTICATION"])
        cfs += "%s = %s\n" %("SEC_DEFAULT_AUTHENTICATION_METHODS", self.configuration["SEC_DEFAULT_AUTHENTICATION_METHODS"])
        cfs += "%s = %s\n" %("SEC_ENABLE_MATCH_PASSWORD_AUTHENTICATION", self.configuration["SEC_ENABLE_MATCH_PASSWORD_AUTHENTICATION"])
        cfs += "%s = %s\n" %("SEC_DEFAULT_ENCRYPTION", self.configuration["SEC_DEFAULT_ENCRYPTION"])
        cfs += "%s = %s\n" %("SEC_DEFAULT_INTEGRITY", self.configuration["SEC_DEFAULT_INTEGRITY"])
        cfs += "%s = %s\n" %("ALLOW_WRITE", self.configuration["ALLOW_WRITE"])
        cfs += "%s = %s\n" %("ALLOW_ADMINISTRATOR", self.configuration["ALLOW_ADMINISTRATOR"])
        cfs += "%s = %s\n" %("NUM_SLOTS", self.configuration["NUM_SLOTS"])
        if self.auth == 'password':
            self.log.info("Password auth requested...")
            cfs += "%s = %s\n" %("SEC_DEFAULT_AUTHENTICATION_METHODS", self.configuration["SEC_DEFAULT_AUTHENTICATION_METHODS"])
            cfs += "%s = %s\n" %("SEC_PASSWORD_FILE", self.configuration["SEC_PASSWORD_FILE"])
            cmd = "condor_store_cred -f %s/condor_password -p %s" % (self.condor_dir, 
                                                                     self.password)
            self.runcommand(cmd)
            self.log.info("Password file created successfully. ")
        elif self.auth == 'gsi':
            self.log.info("GSI auth requested...")
            cfs += "%s = %s\n" %("SEC_DEFAULT_AUTHENTICATION_METHODS", self.configuration["SEC_DEFAULT_AUTHENTICATION_METHODS"])
            cfs += "%s = %s\n" %("GSI_DAEMON_DIRECTORY", self.configuration["GSI_DAEMON_DIRECTORY"])
            cfs += "%s = %s\n" %("GSI_DAEMON_TRUSTED_CA_DIR", self.configuration["GSI_DAEMON_TRUSTED_CA_DIR"])
            cfs += "%s = %s\n" %("GSI_DAEMON_PROXY", self.configuration["GSI_DAEMON_PROXY"])
            cfs += "%s = %s\n" %("GSI_DAEMON_NAME", self.configuration["GSI_DAEMON_NAME"])
            self.log.debug("GSI_DAEMON_NAME=%s" % self.authtok )
            cfs += "%s = %s\n" %("GRIDMAP", self.configuration["GRIDMAP"])
            self.populate_gridmap()

        
        lc = open(lconfig, 'a')
        lc.write(cfs)
        lc.close()


    def run_condor_master(self):
        self.log.info("Running condor_master...")
        cmd = "condor_master -f -pidfile %s/master.pid &" % self.condor_dir
        self.log.debug("cmd = %s" % cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        masterpid = p.pid
        time.sleep(300)
        (out, err) = p.communicate()
        self.log.info("Condor_master has returned...")
        

    def cleanup(self):
        try:
            os.chdir(self.iwd)
            cd = self.condor_dir
            self.log.info("Removing temporary directory: %s" % cd)
            shutil.rmtree(cd)
            self.log.debug("Done remove temp dir.")            
        except Exception, ex:
            self.log.error("Exception caught during cleanup. Ex: %s" % ex)
            raise ex
    #
    # Utilities
    #
    def runcommand(self,cmd):
        self.log.debug("cmd = %s" % cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (out, err) = p.communicate()
        if p.returncode == 0:
            self.log.info('Command return OK.')
            self.log.debug(out)
        else:
            self.log.error(err)
            raise Exception("External command failed. Job failed.") 
        return out

if __name__ == '__main__':
    usage = """
    usage: $0 [options]

Run glidein against given collector:port and auth for at least -x seconds. 

OPTIONS:
    -h --help           Print help.
    -d --debug          Debug logging.      
    -v --verbose        Verbose logging. 
    -c --collector      Collector name
    -p --port           Collector port
    -a --authtype       Auth [password|gsi]
    -t --authtoken      Auth token (password or comma-separated subject DNs for GSI)
    -x --lingertime     Glidein linger time seconds [300]
"""

    
    # Defaults
    condor_version="8.0.6"
    condor_urlbase="http://dev.racf.bnl.gov/dist/condor"
    collector_host="gridtest05.racf.bnl.gov"
    collector_port= "29618"
    authtype="gsi"
    authtoken="/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=Services/CN=gridtest3.racf.bnl.gov, /DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=Services/CN=gridtest5.racf.bnl.gov "
    lingertime="600"   # 10 minutes
    startexpression = "TRUE"
    loglevel=logging.DEBUG
    
    # Handle command line options
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 
                                   "hdvc:p:a:t:x:r:u:", 
                                   ["help",
                                    "debug",
                                    "verbose", 
                                    "collector=", 
                                    "port=", 
                                    "authtype=",
                                    "authtoken=",
                                    "lingertime=",
                                    "condorversion=",
                                    "condorurlbase=",
                                    "startexpression="
                                    ])
    except getopt.GetoptError, error:
        print( str(error))
        print( usage )                          
        sys.exit(1)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(usage)                     
            sys.exit()            
        elif opt in ("-d", "--debug"):
            loglevel = logging.DEBUG
        elif opt in ("-v", "--verbose"):
            loglevel = logging.INFO
        elif opt in ("-c", "--collector"):
            collector_host = arg
        elif opt in ("-p", "--port"):
            collector_port = int(arg)
        elif opt in ("-a", "--authtype"):
            authtype = arg
        elif opt in ("-t", "--authtoken"):
            authtoken = arg
        elif opt in ("-x","--lingertime"):
            lingertime = int(arg)
        elif opt in ("-r", "--condorversion"):
            condor_version = arg
        elif opt in ("-u", "--condorurlbase"):
            condor_urlbase = arg
        elif opt in ("--startexpression"):
            startexpression = arg
            
    try:
        gi = CondorGlidein(condor_version=condor_version, 
                   condor_urlbase=condor_urlbase,
                   collector=collector_host,
                   port=collector_port,
                   auth=authtype, 
                   token=authtoken, 
                   linger=lingertime, 
                   startexpression=startexpression,
                   loglevel=loglevel )
        gi.run_condor_master()
        gi.cleanup()
    except Exception, ex:
        self.log.error("Top-level exception: %s" % ex)
        gi.cleanup()



###########################
#
# example of START expression
#
#   START = ( ( $(HIGHCAS_START) || \
#             ($(GEN_QUEUE)||$(LOWCAS_START)) ) && \
#             $(BLOCKED_USERS) && \
#             $(Turn_Off) == False )
#   
