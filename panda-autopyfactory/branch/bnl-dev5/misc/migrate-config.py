#!/bin/env python
#
# Quick-and-dirty script to generate APF 2.x configuration files from 1.3 configuration. 
#
#

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
batchsubmitplugin = CondorGT2
# batchsubmitplugin = CondorCREAM
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

usage = ''' migrate-config.py <oldconfig>
            creates APF config files (factory, queues, proxy) based on v1.3 factory.conf
'''


import sys

if len(sys.argv) < 2:
    print(usage)






