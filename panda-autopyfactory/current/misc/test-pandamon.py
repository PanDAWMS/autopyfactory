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
   
'''

SERVER='panda.cern.ch'
PORT='25980'
SVCPATH='/server/pandamon/query?'

def runtest1():
    print("Running test...")
    (h, a, n )= socket.gethostbyaddr( socket.gethostbyname(platform.node()) )
    #h = host, a = short alias list,  n= ip address list
    tnow = datetime.datetime.utcnow()

    attributemap = { 'autopilot' : 'updateservicelist',
                     'status'    : 'running',
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
                }
    print(attributemap)

    q = ''
    for k in attributemap.keys():
        q += "&%s=%s" % (k, urllib.quote_plus(str(attributemap[k])) )    
    qurl='http://%s:%s%s%s' % ( SERVER,
                                PORT,
                                SVCPATH,
                                q
                               )
    print("%s" % qurl)
    r = urllib2.Request(url=qurl)
    #r.add_header('User-Agent', 'awesome fetcher')
    #r.add_data(urllib.urlencode({'foo': 'bar'})
    #response = urlopen(r)
    

    
    
    
    

if __name__ == '__main__':
    runtest1()
    
    
    
