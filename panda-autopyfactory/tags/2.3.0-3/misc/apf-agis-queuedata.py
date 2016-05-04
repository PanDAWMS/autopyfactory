#!/bin/env python
#
# All schedconfig: 
# http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all
# get panda_queue_name
# for all panda_queue_name
#    http://panda.cern.ch:25880/server/pandamon/query?tpmes=pilotpars&queue=BNL_ATLAS_2-condor 
#

import urllib2
import simplejson as json

SCALL="http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all"


def sourcescript_from_copysetup(input):
    if len(input) > 2:
        caretidx = input.find('^')
        if caretidx > -1:
            return input[:caretidx]
        else:
            return input
    else:
        return ""

def main():
    response = urllib2.urlopen(SCALL)
    schedconfall = response.read()
    #print(schedconfall)
    
    result_object = json.loads(schedconfall)
    keys = result_object.keys()
    
    keys = sorted(keys)
    for qname in keys:
        qdata = result_object[qname]
        print(qname)
        for sk in sorted(qdata.keys()):
            #print('      %s=%s' % (sk,qdata[sk]) )
            if sk in ["copytool", "copytoolin", "envsetup", "envsetupin","cloud_for_panda"]:
                print('      %s=%s' % (sk,qdata[sk]) )
            elif sk in ["copysetup", "copysetupin"]:
                print('      %s=%s' % (sk, sourcescript_from_copysetup(qdata[sk])))



           
            
if __name__ == '__main__':
    main()