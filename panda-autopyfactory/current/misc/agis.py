#!/usr/bin/env python 

from urllib import urlopen
import sys
try:
    import json as json
except ImportError, err:
    import simplejson as json

from ConfigParser import SafeConfigParser


#  ---   read config filename from  input ---

if len(sys.argv) != 2:
    print 'config filename missing'
    sys.exit(1)
else:
    configfile = sys.argv[1]


#  ---   read AGIS content ---
url = 'http://atlas-agis-api-dev.cern.ch/request/pandaqueue/query/list/?json&preset=full&ceaggregation'
handle = urlopen(url)
# json always gives back unicode strings (eh?) - convert unicode to utf-8
jsonData = json.load(handle, 'utf-8')
handle.close()

agisdict = {}
for jsonDict in jsonData:
    key = jsonDict['panda_queue_name']
    agisdict[key] = jsonDict

#  ---   Load the config file ---

conf = SafeConfigParser()
conf.readfp(open(configfile))


for section in conf.sections():
    key = conf.get(section, 'batchqueue')
    agis_sect = agisdict[key]
    wmsqueue = agis_sect['panda_resource']
    for q in agis_sect['queues']:
        if q['ce_flavour'] == 'OSG-CE':
            gridresource = '%s/jobmanager-%s' %(q['ce_endpoint'], q['ce_jobmanager'])
            if q['ce_version'] == 'GT2':
                submitplugin = 'condorgt2'
            if q['ce_version'] == 'GT5':
                submitplugin = 'condorgt5'
        if q['ce_flavour'] == 'CREAM-CE':
            gridresource = '%s/ce-cream/services/CREAM2 %s %s' %(q['ce_endpoint'], q['ce_jobmanager'], q['ce_queue_name'])
            submitplugin = 'condorcream'
        print 
        print '[%s]' %q['ce_name']
        print 'autofill = True'
        print 'override = True'
        print 'batchqueue = %s' %key
        print 'wmsqueue = %s' %wmsqueue
        print 'batchsubmit.%s.gridresource = %s' %(submitplugin, gridresource)

    

