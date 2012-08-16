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
import socket
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
   

   
'''

SERVER='panda.cern.ch'
PORT='25980'
SVCPATH='/server/pandamon/query?'

def runtest1():
    print("Running service update...")
    (h, a, n )= socket.gethostbyaddr( socket.gethostbyname(platform.node()) )
    #h = host, a = short alias list,  n= ip address list
    tnow = datetime.datetime.utcnow()

    am = { 'status'    : 'running',
           'name'      : 'Job scheduler',
           'grp'       : 'TestPilot',
           'type'      : 'tpmon',
           'pid'       : os.getpid(),
           'userid'    : pwd.getpwuid(os.getuid()).pw_name,
           'doaction'  : '',
           'host'      : h,
           'tstart'    : datetime.datetime.utcnow(),
           'lastmod'   : datetime.datetime.utcnow(),
           'message'   : '',
           'config'    : 'BNL-CLOUD-condor',
           #   config=pilotScheduler.py+--queue%3DANALY_NET2-pbs+--pandasite%3DANALY_NET2+--pilot%3DatlasOfficial2&
           'description': 'TestPilot service',
           'cyclesec'      : '360'          
           }
    sendQuery(am)


def runtest2():
    print("Running job update test...")
    
    (host, alias, n )= socket.gethostbyaddr( socket.gethostbyname(platform.node()) )
    #h = host, a = short alias list,  n= ip address list
     
    am = {
          'status'        : 'active',
          'state'         : 'submitted',
          'queueid'       : 'BNL_CLOUD',
          'tsubmit'       : datetime.datetime.utcnow(),
          'workernode'    : 'unassigned',
          'tpid'          : "%d.1" %  (random.random() * 100000 ),
          'nickname'      : 'BNL_CLOUD' ,  # actually panda queuename, i.e. with -condor, etc. 
          'url'           : 'http://gridtest03.racf.bnl.gov:25880/2012-08-16/BNL_CLOUD',
          'user'          : pwd.getpwuid(os.getuid()).pw_name,
          'tcheck'        : datetime.datetime.utcnow(),
          'system'        : 'osg',
          'jobid'         : "%d.1" %  (random.random() * 100000 ),
          'host'          : host,
          'submithost'    : alias[0],
          'tenter'        : datetime.datetime.utcnow(),
          'schedd_name'   : host,
          'type'          : 'atlasOfficial2',
          'tstate'        : datetime.datetime.utcnow(),
          'errinfo'       : '',          
          }
    sendQuery(am, 'updatepilot')


def sendQuery(attributemap, querytype='updateservicelist'):
    '''
    querytype:   updateservicelist | updatepilot
    
    
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
    runtest1()
    runtest2()
    
    
