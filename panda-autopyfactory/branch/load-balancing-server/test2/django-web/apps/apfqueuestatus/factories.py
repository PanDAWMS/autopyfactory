import json
import logging
import time
import os



class Singleton(type):
    def __init__(cls, name, bases, dct):
        cls.__instance = None
        type.__init__(cls, name, bases, dct)
    def __call__(cls, *args, **kw):
        if cls.__instance is None:
            cls.__instance = type.__call__(cls, *args,**kw)
        return cls.__instance



############################### ###############################

import datetime
import xml.dom.minidom


GRAM2STATUS = {'1':   'gPENDING',
               '2':   'gACTIVE',
               '4':   'gFAILED',
               '8':   'gDONE',
               '16':  'gSUSP',
               '32':  'gUNSUB',
               '64':  'gSTAGE_IN',
               '128': 'gSTAGE_OUT'}
            
CONDOR2STATUS = {'0': 'jUNSUB',
                 '1': 'jIDLE',
                 '2': 'jRUNNING',
                 '3': 'jREMOVED',
                 '4': 'jCOMPLETE',
                 '5': 'jHELD',
                 '6': 'jERROR'}

def listnodesfromxml(xmldoc, tag):
	return xmldoc.getElementsByTagName(tag)

def node2dict(node):

        dic = {}
        for child in node.childNodes:
            if child.nodeType == child.ELEMENT_NODE:
                key = child.attributes['n'].value
                # the following 'if' is to protect us against
                # all condor_q versions format, which is kind of 
                # weird:
                #       - there are tags with different format, with no data
                #       - jobStatus doesn't exist. But there is JobStatus
                if len(child.childNodes[0].childNodes) > 0:
                    value = child.childNodes[0].firstChild.data
                    dic[key.lower()] = str(value)
        return dic


def aggregateinfo( input):

    queues = {}
    for item in input:
        if not item.has_key('match_apf_queue'):
            # This job is not managed by APF. Ignore...
            continue
        apfqname = item['match_apf_queue']
        # get current dict for this apf queue
        try:
            qdict = queues[apfqname]
        # Or create an empty one and insert it.
        except KeyError:
            qdict = {}
            queues[apfqname] = qdict    
        
        # Iterate over attributes and increment counts...
        for attrkey in item.keys():
            # ignore the match_apf_queue attrbute. 
            if attrkey == 'match_apf_queue':
                continue
            attrval = item[attrkey]
            # So attrkey : attrval in joblist
            
            
            # Get current attrdict for this attribute from qdict
            try:
                attrdict = qdict[attrkey]
            except KeyError:
                attrdict = {}
                qdict[attrkey] = attrdict
            
            try:
                curcount = qdict[attrkey][attrval]
                qdict[attrkey][attrval] = curcount + 1                    
            except KeyError:
                qdict[attrkey][attrval] = 1
    return queues


statuskeys = {}
statuskeys['globusstatus'] = ['gUNSUB',    
                              'gPENDING',
                              'gSTAGE_IN',
                              'gACTIVE', 
                              'gSTAGE_OUT',  
                              'gSUSP',  
                              'gDONE',  
                              'gFAILED']
statuskeys['jobstatus'] = ['jUNSUB',    
                           'jIDLE',
                           'jRUNNING',
                           'jCOMPLETE', 
                           'jHELD',  
                           'jERROR',  
                           'jREMOVED']






def map2table(log, aggdict):

    queuetable = {}
    for site in aggdict.keys():
        queuetable[site] = {}
        sitedict = aggdict[site]

        qi = {'gPENDING': 0 , 
              'gACTIVE': 0, 
              'gFAILED' : 0 , 
              'gDONE': 0,
              'gSUSP': 0,
              'gUNSUB' : 0,
              'gSTAGE_IN' : 0,
              'gSTAGE_OUT' : 0,                               
             }

        if 'globusstatus' in sitedict.keys():
            # fill in values here
            sd = sitedict['globusstatus']
            for status in sd.keys():
                try:
                    qi[GRAM2STATUS[status]] += sd[status]
                except KeyError:
                    pass
                    #log.warn("Got globusstatus of %s" % status)
        
        queuetable[site]['globusstatus'] = qi

        qi = {'jUNSUB': 0 , 
              'jIDLE': 0, 
              'jRUNNING' : 0 , 
              'jREMOVED': 0,
              'jCOMPLETE': 0,
              'jHELD' : 0,
              'jERROR' : 0,                              
             }

        if 'jobstatus' in sitedict.keys():
            # fill in values
            sd = sitedict['jobstatus']
            for status in sd.keys():
                try:
                    qi[CONDOR2STATUS[status]] += sd[status]
                except KeyError:
                    pass
                    #log.warn("Got jobstatus of %s" % status)
            
        queuetable[site]['jobstatus'] = qi              

    return queuetable




    
def printtable(log, queuetable):
    #print(datetime.datetime.now().strftime("%a %b %d %H:%M:%S %Y"))
    #out = '-----------------------------------------------------------------\n'
    out = ""
    sitewidth = 28
    ks = queuetable.keys()
    ks.sort()
    for s in ks:
        sitename = s
        # format sitename
        w = len(sitename)
        addl = sitewidth - w
        sitename = sitename + (' ' * addl)
        out += '%s \t' %sitename 
        qi = queuetable[s]

        #if 'globusstatus' in qi.keys():
        t = qi['globusstatus']
        keys = statuskeys['globusstatus']
        for k in keys:
           out += "%s = %s\t" %(k,t[k])
        #if 'jobstatus' in qi.keys():
        t = qi['jobstatus']
        keys = statuskeys['jobstatus']
        for k in keys:
           out += "%s = %s\t" %(k,t[k])

        out += "\n"
    return out


def bprinttable(table):

    lines = []
    qs = table.keys()
    qs.sort()
    for q in qs:
        line = '%s ' %q
        info = table[q]

        #if 'globusstatus' in qi.keys():
        t = info['globusstatus']
        keys = statuskeys['globusstatus']
        for k in keys:
           line += "%s " %t[k]
        #if 'jobstatus' in qi.keys():
        t = info['jobstatus']
        keys = statuskeys['jobstatus']
        for k in keys:
           line += "%s " %t[k]

        lines.append(line)
        
    return lines






############################### ###############################



class InfoManager:
    __metaclass__ = Singleton

    """
    This class manages factories information.
    The information is stored into two objects:
        -- one for the factories
        -- one for the queues
    These two objects share a lot of common information,
    but having them twice makes reading it much faster in all cases.
    """

    def __init__(self):
        
        self._logger()

        self.tables = {}
        self.log.debug("object InfoManager created")

    def _logger(self):

        self.log = logging.getLogger('main')
        lf = '/var/log/apfqueuestatus/log'
        logdir = os.path.dirname(lf)
        if not os.path.exists(logdir):
           os.makedirs(logdir)
        logStream = logging.FileHandler(filename=lf)
        FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'
        formatter = logging.Formatter(FORMAT)
        formatter.converter = time.gmtime  # to convert timestamps to UTC  
        logStream.setFormatter(formatter)
        self.log.addHandler(logStream) 
        self.log.setLevel(logging.DEBUG)


    def add(self, data):
        """
        """

        #self.log.info('adding data: %s' %data)
        self.data = json.loads(data)
        #self.log.info('adding data: %s' %self.data)

        factory = self.data['factory']
        info = self.data['info']

        xmldoc = xml.dom.minidom.parseString(info).documentElement
        nodelist = []
        for c in listnodesfromxml(xmldoc, 'c') :
            node_dict = node2dict(c)
            nodelist.append(node_dict)  
        if len(nodelist) > 0:
            aggdict = aggregateinfo(nodelist)
            self.tables[factory] = map2table(self.log, aggdict)
            #self.log.debug('table for factory: %s is %s' %(factory, self.tables[factory]))


    def get(self, data=None):
        """
        """

        #self.log.debug('retrieving... %s' %self.table)
        #out = json.dumps(self.table)
        #self.log.debug('retrieving... %s' %self.tables)

        out = ""
        factories = self.tables.keys()
        factories.sort()
        #for factory, table in self.tables.iteritems():
        for factory in factories:
            table = self.tables[factory]
            out += 'Factory: %s\n' %factory
            out += printtable(self.log, table)
            out += '\n'

        self.log.debug('retrieving... %s' %out)
	out = json.dumps(out)

        return out


    def bget(self):

        tables = {}
        factories = self.tables.keys()
        factories.sort()
        for factory in factories:
            table = self.tables[factory]
            #table = printtable(self.log, table)
            table = bprinttable(table)
            tables[factory] = table
        return tables

