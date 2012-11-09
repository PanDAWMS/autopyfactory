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
#url = 'http://atlas-agis-api-dev.cern.ch/request/pandaqueue/query/list/?json&preset=full&ceaggregation'
url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all'

handle = urlopen(url)
# json always gives back unicode strings (eh?) - convert unicode to utf-8
jsonData = json.load(handle, 'utf-8')
handle.close()

###agisdict = {}
###for jsonDict in jsonData:
###    key = jsonDict['panda_queue_name']
###    agisdict[key] = jsonDict

#  ---   Load the config file ---

conf = SafeConfigParser()
conf.readfp(open(configfile))


for section in conf.sections():
    key = conf.get(section, 'batchqueue')
    #agis_sect = agisdict[key]
    agis_sect = jsonData[key]
    wmsqueue = agis_sect['panda_resource']
    type = agis_sect['type']
    for q in agis_sect['queues']:
        gramqueue = None 

        if q['ce_flavour'] == 'OSG-CE':

            gridresource = '%s/jobmanager-%s' %(q['ce_endpoint'], q['ce_jobmanager'])
            if q['ce_version'] == 'GT2':
                submitplugin = 'CondorGT2'
                submitpluginstring = 'condorgt2'
                gramversion = 'gram2'
            if q['ce_version'] == 'GT5':
                submitplugin = 'CondorGT5'
                submitpluginstring = 'condorgt5'
                gramversion = 'gram5'
            if q['ce_queue_name']:
                gramqueue = q['ce_queue_name']

        elif q['ce_flavour'] == 'CREAM-CE':

            gridresource = '%s/ce-cream/services/CREAM2 %s %s' %(q['ce_endpoint'], q['ce_jobmanager'], q['ce_queue_name'])
            submitplugin = 'CondorCREAM'
            submitpluginstring = 'condorcream'

        elif q['ce_flavour'] == 'LCG-CE':

            gridresource = '%s/jobmanager-%s' %(q['ce_endpoint'], q['ce_jobmanager'])
            submitplugin = 'CondorGT2'
            submitpluginstring = 'condorgt2'
            gramversion = 'gram2'
            gramqueue = q['ce_queue_name']
            
        else:
            # ce_flavour has no value or not yet understood
            continue

        print 
        #print '[%s-%s]' %(section, q['ce_name'])
        #print '[%s-%s-%s]' %(section, q['ce_endpoint'].split(':')[0], q['ce_queue_name'])
        print '[%s-%s]' %(agis_sect['panda_queue_name'], q['ce_queue_id'])
        print 'autofill = False'
        print 'batchqueue = %s' %key
        print 'wmsqueue = %s' %wmsqueue
        print 'batchsubmitplugin = %s' %submitplugin
        print 'batchsubmit.%s.gridresource = %s' %(submitpluginstring, gridresource)
        if gramqueue:
            print 'globusrsl.%s.queue = %s' %(gramversion, gramqueue)
        if type == 'analysis':
            print 'batchsubmit.%s.proxy = atlas-analysis' %submitpluginstring
        elif type == 'production':
            print 'batchsubmit.%s.proxy = atlas-production' %submitpluginstring


    

