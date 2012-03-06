#!/bin/env python
#
# Quick-and-dirty script to generate APF 2.x configuration files from 1.3 configuration. 
#
#
import sys
from ConfigParser import ConfigParser
DEBUG=False




FACTORY_DEFAULT = '''
[Factory]
factoryAdminEmail = neo@matrix.net
factoryId = CHANGEME
factoryUser = apf
versionTag = 2.1.0

monitorURL =  http://apfmon.lancs.ac.uk/mon/

queueConf = file:///etc/apf/queues.conf
proxyConf = /etc/apf/proxy.conf
proxymanager.enabled = True

cycles = None

factory.sleep=30
wmsstatus.panda.sleep = 50
batchstatus.condor.sleep = 50

baseLogDir = /home/apf/factory/logs
baseLogDirUrl = http://my.host.domain:25880
baseLogHttpPort = 25880 

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
batchsubmitplugin = CondorCREAM
schedplugin = Activated
proxy = atlas-production

batchsubmit.condorgt2.proxy = atlas-production
batchsubmit.condorgt5.proxy = atlas-production
batchsubmit.condorcream.proxy = atlas-production
batchsubmit.condorlocal.proxy = atlas-production

batchsubmit.condorgt2.condor_attributes = periodic_hold=GlobusResourceUnavailableTime =!= UNDEFINED &&(CurrentTime-GlobusResourceUnavailableTime>30),periodic_remove = (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > 3600) || (JobStatus == 1 && globusstatus =!= 1 && (CurrentTime - EnteredCurrentStatus) > 86400)

apfqueue.sleep = 360

executable = /usr/libexec/wrapper.sh
executable.wrappervo = ATLAS
executable.wrappertarballurl = http://dev.racf.bnl.gov/dist/wrapper/wrapper.tar.gz
executable.wrapperserverurl = http://pandaserver.cern.ch:25080/cache/pilot
executable.wrapperloglevel = debug

override = False

'''

usage = ''' migrate-config.py <old-factory.conf>
  Creates APF config files (factory, queues, proxy) based on v1.3 factory.conf
'''

SPECIAL_SECTIONS = ['Factory','Pilots','QueueDefaults']


def generate_configs(filename):
    '''
    -- Reads in factory.conf into configparser
    -- pulls out all Queudefaults and puts in queues.conf DEFAULTS
    -- pulls out Factory and Pilots and merges into factory.conf
    -- reads all other sections and translates to queues.conf sections. 
       
    '''

    cp = ConfigParser()
    cp.read(filename)
    
    # Handle factory.conf
    fc = open('factory.conf-new','w')
    fc.write(FACTORY_DEFAULT)
    
    #
       
    #for s in cp.sections():
    #    fc.write('[%s]' % s)
    #    for k in cp.items(s):
    #        fc.write("%s = %s" % (k, cp.get(s,k)))
    fc.close()
     
    fq = open('queues.conf-new', 'w')
    fp = open('proxy.conf-new','w')
      
    fq.write(QUEUES_DEFAULT)
    fq.close()
    
    fp.write(PROXY_DEFAULT)
    fp.close()

if __name__ == '__main__':

    if len(sys.argv) < 2:
        print(usage)
    elif sys.argv[1] == "-h" or sys.argv[1] == "--help":
        print(usage)
    else:
        try:
            infile = sys.argv[1]
            generate_configs(infile)
        except:
            print("ERROR: Something wrong with input file: '%s'" % infile)
    
    






