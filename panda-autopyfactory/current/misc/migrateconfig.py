#!/bin/env python
#
# Quick-and-dirty script to generate APF 2.x configuration files from 1.3 
# configuration. 
#
#
import sys
from ConfigParser import ConfigParser
DEBUG=False



FACTORY_DEFAULT = '''
[Factory]
# factoryId = SITE-host-admin
factoryUser = apf
versionTag = 2.1.0

monitorURL =  http://apfmon.lancs.ac.uk/mon/

queueConf = file:///etc/apf/queues.conf
#queueConf = file:///home/apf/etc/queues.conf
proxyConf = /etc/apf/proxy.conf
#proxyConf = ~/etc/proxy.conf
proxymanager.enabled = True

cycles = None

factory.sleep=30
wmsstatus.panda.sleep = 50
batchstatus.condor.sleep = 50

# baseLogDir = /home/apf/factory/logs
# baseLogDirUrl = http://my.host.domain:25880
# baseLogHttpPort = 25880 

logserver.enabled = True
logserver.index = True

'''


PROXY_DEFAULT = '''
[DEFAULT]
baseproxy = None
usercert=~/.globus/usercert.pem
userkey=~/.globus/userkeynopw.pem
lifetime = 604800
checktime = 3600
minlife = 259200
interruptcheck = 1
renew = True

[atlas-production]
vorole = atlas:/atlas/Role=production
proxyfile = /tmp/prodProxy
'''


QUEUES_DEFAULT = '''
[DEFAULT]
enabled = True
status = online
autofill = True

batchstatusplugin = Condor
wmsstatusplugin = Panda
configplugin = Panda
#batchsubmitplugin = CondorGT2
#batchsubmitplugin = CondorGT5
batchsubmitplugin = CondorCREAM
schedplugin = Activated
proxy = atlas-production

batchsubmit.condorgt2.proxy = atlas-production
batchsubmit.condorgt5.proxy = atlas-production
batchsubmit.condorcream.proxy = atlas-production
batchsubmit.condorlocal.proxy = atlas-production

batchsubmit.condorgt2.condor_attributes = periodic_hold=GlobusResourceUnavailableTime =!= UNDEFINED &&(CurrentTime-GlobusResourceUnavailableTime>30),periodic_remove = (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > 3600) || (JobStatus == 1 && globusstatus =!= 1 && (CurrentTime - EnteredCurrentStatus) > 86400)

apfqueue.sleep = 360

sched.activated.min_pilots_per_cycle = 0
sched.activated.max_jobs_torun = 500
sched.activated.max_pilots_per_cycle = 50
sched.activated.max_pilots_pending = 50

executable = /usr/libexec/wrapper.sh
executable.wrappervo = ATLAS
executable.wrappertarballurl = http://dev.racf.bnl.gov/dist/wrapper/wrapper.tar.gz
executable.wrapperserverurl = http://pandaserver.cern.ch:25080/cache/pilot
executable.wrapperloglevel = debug
executable.arguments = --script=pilot.py --libcode=pilotcode.tar.gz,pilotcode-rc.tar.gz --pilotsrcurl=http://panda.cern.ch:25880/cache -f false -m false --user user

override = True

'''

#
# In old factory.conf, [QUEUENAME] should map to wmsqueue
# 

FACTORY_MAPPINGS = { 'factoryowner' : 'factoryAdminEmail',
             'baselogdir' : 'baseLogDir',
             'baselogdirurl' : 'baseLogDirUrl',
            }

QUEUE_MAPPINGS =  { 'nickname' : 'batchqueue',
             'localqueue' : 'batchsubmit.condorcream.queue',
             'queue' : 'batchsubmit.condorcream.gridresource',
             'special-par' : 'batchsubmit.condorcream.queue',
             'siteid' : 'wmsqueue',
             'environ' : 'batchsubmit.condorcream.environ'
            }

USAGE = ''' migrate-config.py <old-factory.conf>
  Creates APF config files (factory, queues, proxy) based on v1.3 factory.conf
  NOTE: Assumes CEs are CREAM CEs. 
  
  
'''


def generate_configs(filename):
    '''
    -- Reads in factory.conf into configparser
    -- pulls out all Queudefaults and puts in queues.conf DEFAULTS
    -- pulls out Factory and Pilots and merges into factory.conf
    -- reads all other sections and translates to queues.conf sections. 
       
    '''
    if DEBUG: print("Reading %s" % filename)
    fccp = ConfigParser()
    fccp.read(filename)
    if DEBUG: print("Made configparser from %s" % filename)
    
    factory = {}
    if fccp.has_section('Factory'):
        if DEBUG: print("Input config has Factory section...")
        for (k,v) in fccp.items('Factory'):
            factory[k] = v
            if DEBUG: print("item: %s = %s" % (k,v))

    pilots = {}
    if fccp.has_section('Pilots'):
        if DEBUG: print("Input config has Pilots section...")
        for (k,v) in fccp.items('Pilots'):
            pilots[k] = v
            if DEBUG: print("item: %s = %s" % (k,v))    
    
    qdefaults = {}
    if fccp.has_section('QueueDefaults'):
        if DEBUG: print("Input config has QueueDefaults section...")
        items = fccp.items("QueueDefaults")
        if DEBUG: print("QueueDefault section has %d items" % len(items))
        for (k,v) in fccp.items('QueueDefaults'):
            qdefaults[k] = v
            if DEBUG: print("item: %s = %s" % (k,v))
    
    
     
    # Start factory.conf
    fc = open('factory.conf-new','w')
    fc.write(FACTORY_DEFAULT)

    # Start queues.conf
    fq = open('queues.conf-new', 'w')
    fq.write(QUEUES_DEFAULT)

    # Start, and finish, proxy.conf
    fp = open('proxy.conf-new','w')    
    fp.write(PROXY_DEFAULT)
    fp.close()    
    
    # Handle inbound config
    
    
    for s in fccp.sections():
        if s == 'Factory':
            if DEBUG: print("Handling Factory section...")
            for (k,v) in fccp.items(s):
                if DEBUG: print("Handling key '%s'..." % k)
                if k in FACTORY_MAPPINGS.keys():
                    fc.write("%s = %s\n" % ( FACTORY_MAPPINGS[k], v))
                    if DEBUG: print("Adding '%s = %s' ..." %  ( FACTORY_MAPPINGS[k], v ))
            
        elif s == 'Pilots':
            if DEBUG: print("Handling Pilots section...")
            for (k,v) in fccp.items(s):
                if DEBUG: print("Handling key '%s'..." % k)
                if k in FACTORY_MAPPINGS.keys():
                    fc.write("%s = %s\n" % ( FACTORY_MAPPINGS[k], v))
                    if DEBUG: print("Adding '%s = %s' ..." %  ( FACTORY_MAPPINGS[k], v ))
             
        elif s == 'QueueDefaults':
            if DEBUG: print("Handling Queuedefaults section...")
             
        else:
            if DEBUG: print("Handling [%s] section..." % s)
            fq.write("\n[%s]\n" % s)
            fq.write("wmsqueue = %s\n" % s)
            for (k,v) in fccp.items(s):
                if DEBUG: print("Handling key '%s'..." % k)
                if k in QUEUE_MAPPINGS.keys():
                    fq.write("%s = %s\n" % ( QUEUE_MAPPINGS[k], v))
                    if DEBUG: print("Adding '%s = %s' ..." %  ( QUEUE_MAPPINGS[k], v ))
                
    
    
    
     
     
     
    fc.close()
    fq.close()




if __name__ == '__main__':

    if len(sys.argv) < 2:
        print(USAGE)
    elif sys.argv[1] == "-h" or sys.argv[1] == "--help":
        print(USAGE)
    else:
        if "-d" in sys.argv or "--debug" in sys.argv: DEBUG=True
        try:
            for a in sys.argv[1:]:
                if a.startswith("-"):
                    pass
                else:
                    infile = a
            generate_configs(infile)
        except:
            print("ERROR: Something wrong with input file: '%s'" % infile)
    
    






