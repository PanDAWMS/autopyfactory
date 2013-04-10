#!/usr/bin/env python
#
# urllib.urlencode({'abc':'d f', 'def': '-!2'})
# 'abc=d+f&def=-%212'
# urllib.quote_plus()
#
#  r = Request(url='http://www.mysite.com')
#  r.add_header('User-Agent', 'awesome fetcher')
#  r.add_data(urllib.urlencode({'foo': 'bar'})
#  response = urlopen(r)
#
#  datetime.datetime(2000,1,1)
# 
#  socket.gethostbyname(platform.node())
#  socket.gethostbyaddr("69.59.196.211")
#
#  (h, a, n )= socket.gethostbyaddr( socket.gethostbyname(platform.node()) )
#   h = host, a = short alias list,  n= ip address list
#   

import os
import platform
import pwd
import random
import socket
import sys
import urllib
import urllib2
import datetime

'''
curl --connect-timeout 20 --max-time 180 -sS 
   'http://panda.cern.ch:25980/server/pandamon/query?
   autopilot=updateservicelist&
   status=running&
   name=Job+scheduler&
   grp=TestPilot&
   type=tpmon&
   pid=12345&
   userid=sm&
   doaction=&
   host=gridui10.usatlas.bnl.gov&
   tstart=2012-08-14+10%3A17%3A14.900791&
   tstop=2000-01-01+00%3A00%3A00&
   message=&
   lastmod=2012-08-14+10%3A17%3A14.900791&
   config=pilotScheduler.py+--queue%3DANALY_NET2-pbs+--pandasite%3DANALY_NET2+--pilot%3DatlasOfficial2&
   description=TestPilot+service'
   

   curl --connect-timeout 20 --max-time 180 -sS 
   'http://panda.cern.ch:25980/server/pandamon/query?
   autopilot=updatepilot&
   status=active&
   queueid=ANALY_NET2&
   tsubmit=2012-08-14+10%3A21%3A20.295097&
   workernode=unassigned&
   tpid=tp_gridui10_88777_9999999-102119_13&
   url=http%3A%2F%2Fgridui10.usatlas.bnl.gov%3A25880%2Fschedlogs%2Ftp_gridui10_88888_9999999%2Ftp_gridui10_28847_20120814-102119_13&
   nickname=ANALY_NET2-pbs&
   tcheck=2012-08-14+10%3A21%3A20.295375&
   system=osg&jobid=3333333.0&
   tenter=2012-08-14+10%3A21%3A19.521314&
   host=gridui10.usatlas.bnl.gov&
   state=submitted&
   submithost=gridui10&
   user=sm&
   schedd_name=gridui10.usatlas.bnl.gov&
   type=atlasOfficial2&
   tstate=2012-08-14+10%3A21%3A20.295097&
   errinfo=+'   
   

works:
curl --connect-timeout 20 --max-time 180 -sS 
'http://panda.cern.ch:25980/server/pandamon/query?
autopilot=updatepilot&
status=active&
queueid=BNL_CLOUD&
tsubmit=2012-08-15+22%3A58%3A57.528556&
workernode=unassigned&
tpid=9999999.3&
url=http%3A%2F%2Fgridui08.usatlas.bnl.gov%3A25880%2Fschedlogs%2Ftp_gridui12_23036_20120815%2Ftp_gridui12_23036_20120815-225856_624&
nickname=BNL_CLOUD&
tcheck=2012-08-15+22%3A58%3A57.528842&
system=osg&jobid=9999999.0&
tenter=2012-08-15+22%3A58%3A56.771376&
host=gridui08.usatlas.bnl.gov&
state=submitted&
submithost=gridui08&
user=jhover&
schedd_name=gridui08.usatlas.bnl.gov&
type=atlasOfficial2&
tstate=2012-08-15+22%3A58%3A57.528556&
errinfo=+'

NOT working:

http://panda.cern.ch:25980/server/pandamon/query?
autopilot=updatepilot&
status=active&
queueid=BNL_CLOUD&
tsubmit=2012-08-16+18%3A16%3A20.803098&
workernode=unassigned&
tpid=95219.1&
url=http%3A%2F%2Fgridtest03.racf.bnl.gov%3A25880%2F2012-08-16%2FBNL_CLOUD&
type=atlasOfficial2&
tcheck=2012-08-16+18%3A16%3A20.803163&
system=osg&
jobid=14147.1&
tenter=2012-08-16+18%3A16%3A20.803170&
state=submitted&
submithost=gridui08&
user=jhover&
host=gridui08.usatlas.bnl.gov&
schedd_name=gridui08.usatlas.bnl.gov&
nickname=BNL_CLOUD&
tstate=2012-08-16+18%3A16%3A20.803172&
errinfo=

Job status sequence:

[root@gridui12 scheduler]# cat service_gridui12.usatlas.bnl.gov_sm_21388 service_gridui12.usatlas.bnl.gov_sm_707 service_gridui12.usatlas.bnl.gov_sm_1300 | grep tp_gridui12_23036_20120815-225856_624 | grep messageDB
Schedulerutils.messageDB(): cmd= curl --connect-timeout 20 --max-time 180 -sS 'http://panda.cern.ch:25980/server/pandamon/query?autopilot=updatepilot&status=active&queueid=BU_ATLAS_Tier2o&tsubmit=2012-08-15+22%3A58%3A57.528556&workernode=unassigned&tpid=tp_gridui12_23036_20120815-225856_624&url=http%3A%2F%2Fgridui12.usatlas.bnl.gov%3A25880%2Fschedlogs%2Ftp_gridui12_23036_20120815%2Ftp_gridui12_23036_20120815-225856_624&nickname=BU_ATLAS_Tier2o-pbs&tcheck=2012-08-15+22%3A58%3A57.528842&system=osg&jobid=39949228.0&tenter=2012-08-15+22%3A58%3A56.771376&host=gridui12.usatlas.bnl.gov&state=submitted&submithost=gridui12&user=sm&schedd_name=gridui12.usatlas.bnl.gov&type=atlasOfficial2&tstate=2012-08-15+22%3A58%3A57.528556&errinfo=+' 
Schedulerutils.messageDB(): cmd= curl --connect-timeout 20 --max-time 180 -sS 'http://panda.cern.ch:25980/server/pandamon/query?autopilot=updatepilot&status=active&tschedule=2012-08-15+23%3A06%3A18.985095&tpid=tp_gridui12_23036_20120815-225856_624&tcheck=2012-08-15+23%3A06%3A18.985130&jobid=39949228.0&state=scheduled&nickname=BU_ATLAS_Tier2o-pbs&tstate=2012-08-15+23%3A06%3A18.985095' 
Schedulerutils.messageDB(): cmd= curl --connect-timeout 20 --max-time 180 -sS 'http://panda.cern.ch:25980/server/pandamon/query?autopilot=updatepilot&status=active&tschedule=2012-08-15+23%3A09%3A15.139811&tpid=tp_gridui12_23036_20120815-225856_624&tcheck=2012-08-15+23%3A09%3A15.139847&jobid=39949228.0&state=scheduled&nickname=BU_ATLAS_Tier2o-pbs&tstate=2012-08-15+23%3A09%3A15.139811' 
Schedulerutils.messageDB(): cmd= curl --connect-timeout 20 --max-time 180 -sS 'http://panda.cern.ch:25980/server/pandamon/query?autopilot=updatepilot&status=active&tpid=tp_gridui12_23036_20120815-225856_624&tcheck=2012-08-16+05%3A42%3A22.543749&jobid=39949228.0&state=running&tstart=2012-08-16+05%3A42%3A22.543696&nickname=BU_ATLAS_Tier2o-pbs&tstate=2012-08-16+05%3A42%3A22.543696' 
Schedulerutils.messageDB(): cmd= curl --connect-timeout 20 --max-time 180 -sS 'http://panda.cern.ch:25980/server/pandamon/query?autopilot=updatepilot&status=finished&PandaID=1577156287&workernode=atlas-cm2.bu.edu&tpid=tp_gridui12_23036_20120815-225856_624&tcheck=2012-08-16+05%3A43%3A55.515658&host=atlas-cm2.bu.edu&jobid=39949228.0&tdone=2012-08-16+05%3A43%3A55.515617&state=done&errcode=0&message=straggling_pilot_not_on_queue_but_in_DB&nickname=BU_ATLAS_Tier2o-pbs&tstate=2012-08-16+05%3A43%3A55.515617&errinfo=Job+successfully+completed' 
Schedulerutils.messageDB(): cmd= curl --connect-timeout 20 --max-time 180 -sS 'http://panda.cern.ch:25980/server/pandamon/query?autopilot=updatepilot&status=finished&PandaID=1577156287&workernode=atlas-cm2.bu.edu&tpid=tp_gridui12_23036_20120815-225856_624&tcheck=2012-08-16+05%3A45%3A30.689477&host=atlas-cm2.bu.edu&jobid=39949228.0&tdone=2012-08-16+05%3A45%3A30.689436&state=done&errcode=0&message=straggling_pilot_not_on_queue_but_in_DB&nickname=BU_ATLAS_Tier2o-pbs&tstate=2012-08-16+05%3A45%3A30.689436&errinfo=Job+successfully+completed' 
Schedulerutils.messageDB(): cmd= curl --connect-timeout 20 --max-time 180 -sS 'http://panda.cern.ch:25980/server/pandamon/query?autopilot=updatepilot&status=finished&PandaID=1577156287&workernode=atlas-cm2.bu.edu&tpid=tp_gridui12_23036_20120815-225856_624&tcheck=2012-08-16+05%3A47%3A07.572424&host=atlas-cm2.bu.edu&jobid=39949228.0&tdone=2012-08-16+05%3A47%3A07.572383&state=done&errcode=0&message=straggling_pilot_not_on_queue_but_in_DB&nickname=BU_ATLAS_Tier2o-pbs&tstate=2012-08-16+05%3A47%3A07.572383&errinfo=Job+successfully+completed' 






  
'''

SERVER='panda.cern.ch'
PORT='25980'
SVCPATH='/server/pandamon/query?'

def runtest1():
    print("Running service update...")
    (h, a, n )= socket.gethostbyaddr( socket.gethostbyname(platform.node()) )
    #h = host, a = short alias list,  n= ip address list
    tnow = datetime.datetime.utcnow()

    am = { 'status'     : 'running',
           'name'       : 'Job scheduler',
           'grp'        : 'TestPilot',
           'type'       : 'tpmon',
           'pid'        : os.getpid(),
           'userid'     : pwd.getpwuid(os.getuid()).pw_name,
           'doaction'   : '',
           'host'       : h,
           'tstart'     : datetime.datetime.utcnow(),
           'lastmod'    : datetime.datetime.utcnow(),
           'message'    : '',
           'config'     : 'BNL-CLOUD-condor',
           #   config=pilotScheduler.py+--queue%3DANALY_NET2-pbs+--pandasite%3DANALY_NET2+--pilot%3DatlasOfficial2&
           'description': 'TestPilot service',
           'cyclesec'   : '360'          
           }
    sendQuery(am)


def runtest2():
    print("Running job update test...")
    
    (host, alias, n )= socket.gethostbyaddr( socket.gethostbyname(platform.node()) )
    #h = host, a = short alias list,  n= ip address list
    
    jobid= "%d.1" %  (random.random() * 100000 )
     
    am = {
          'status'        : 'active',        # active, or finished
          'state'         : 'submitted',    # or scheduled, running this is equivialent to globus  PENDING, ACTIVE
          'queueid'       : 'BNL_CLOUD',
          'tsubmit'       : datetime.datetime.utcnow(),
          'workernode'    : 'unassigned',
          'host'          : 'unassigned',
          'tpid'          : jobid,
          'nickname'      : 'BNL_CLOUD' ,  # actually panda queuename, i.e. with -condor, etc. 
          'url'           : 'http://gridtest03.racf.bnl.gov:25880/2012-08-16/BNL_CLOUD',
          'user'          : pwd.getpwuid(os.getuid()).pw_name,
          'tcheck'        : datetime.datetime.utcnow(),
          'system'        : 'osg',
          'jobid'         : jobid,
          'submithost'    : alias[0],
          'tenter'        : datetime.datetime.utcnow(),
          'schedd_name'   : host,
          'type'          : 'AutoPyFactory',
          'tstate'        : datetime.datetime.utcnow(),
          'errinfo'       : ' ',     ## MUST HAVE space, or won't work!!!      
          }
    sendQuery(am, 'updatepilot')


def sendQuery(attributemap, querytype='updateservicelist'):
    '''
    querytype:   updateservicelist | updatepilot | currentlyqueued
    
    
    '''
    q = ''
    for k in attributemap.keys():
        q += "&%s=%s" % (k, urllib.quote_plus(str(attributemap[k])) )    
    qurl='http://%s:%s%s%s%s' % ( SERVER,
                                PORT,
                                SVCPATH,
                                'autopilot=%s' % querytype ,
                                q
                               )
    print("%s" % qurl)
    r = urllib2.Request(url=qurl)
    #r.add_header('User-Agent', 'awesome fetcher')
    #r.add_data(urllib.urlencode({'foo': 'bar'})
    response = urllib2.urlopen(r)
    print(response.read())


    

if __name__ == '__main__':
    #runtest1()
    #runtest2()
    usage = '''test-pandamon.py <jobid> <state>
    jobid, e.g.   9999999.2
    state         submitted | scheduled | done
    
    '''
    print("sys.argv = %s" % sys.argv)
    if len(sys.argv < 3):
        print(usage)
     
     
    
