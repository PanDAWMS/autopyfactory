#
#
'''

NOTES

This command updates info for currently existing scheduler entry (one for which you know the id). 
We still don't know how to create a new entry. 


   curl --connect-timeout 20 --max-time 180 -sS 
      'http://panda.cern.ch:25980/server/pandamon/query?
       autopilot=updateservicelist&
       id=38&                               << how does this id get created?
       doaction=None&
       name=Job+scheduler&
       pid=12345&
       userid=sm4&
       host=gridui12.usatlas.bnl.gov
       &config=pilotScheduler.py+--queue=ANALY_NET2-pbs+--pandasite=ANALY_NET2+--pilot=atlasOfficial2&
       cyclesec=9' 


Panda server has whitelisted hostnames for submission. Those must be added out of band. 


'''