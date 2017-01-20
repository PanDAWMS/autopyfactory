#!/usr/bin/env python
'''

NOTES

This command updates info for currently existing scheduler entry (one for which you know the id). 
We still don't know how to create a new entry. 

Panda server has whitelisted hostnames for submission. Those must be added out of band. 

Service info check:

  curl --connect-timeout 20 --max-time 180 -sS 
  'http://panda.cern.ch:25980/server/pandamon/query?
  autopilot=validatehost&
  host=gridui10.usatlas.bnl.gov'



Service registration command. 
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


Job update command:

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

from autopyfactory.interfaces import MonitorInterface


class Panda(MonitorInterface):
    
    def __init__(self, apfqueue, config, section):
        pass
 
 



 
    
    
    
    
    


