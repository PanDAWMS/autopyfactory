#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

import commands
import datetime
import logging
import os
import re
import string
import time
import traceback


from autopyfactory import condor 
from autopyfactory.condor  import mynewsubmit
from autopyfactory import jsd
from autopyfactory.interfaces import BatchSubmitInterface
from autopyfactory.info import JobInfo
import autopyfactory.utils as utils



class CondorBase(BatchSubmitInterface):
    
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
        
        self.log = logging.getLogger("main.batchsubmitplugin[%s]" % apfqueue.apfqname)

        self.apfqueue = apfqueue
        self.apfqname = apfqueue.apfqname
        self.factory = apfqueue.factory
        self.fcl = apfqueue.factory.fcl
        self.mcl = apfqueue.factory.mcl
        
        try:
            self.wmsqueue = qcl.generic_get(self.apfqname, 'wmsqueue')
            self.executable = qcl.generic_get(self.apfqname, 'executable')
            self.factoryadminemail = self.fcl.generic_get('Factory', 'factoryAdminEmail')

            self.factoryid = self.fcl.generic_get('Factory', 'factoryId')
            self.monitorsection = qcl.generic_get(self.apfqname, 'monitorsection')
            self.log.trace("monitorsection is %s" % self.monitorsection)            
            self.monitorurl = self.mcl.generic_get(self.monitorsection, 'monitorURL')
            self.log.trace("monitorURL is %s" % self.monitorurl)
            
            self.factoryuser = self.fcl.generic_get('Factory', 'factoryUser')
            self.submitargs = qcl.generic_get(self.apfqname, 'batchsubmit.condorbase.submitargs')
            self.environ = qcl.generic_get(self.apfqname, 'batchsubmit.condorbase.environ')
            #self.batchqueue = qcl.generic_get(self.apfqname, 'batchqueue')
            self.arguments = qcl.generic_get(self.apfqname, 'executable.arguments')
            self.condor_attributes = qcl.generic_get(self.apfqname, 'batchsubmit.condorbase.condor_attributes')
            self.extra_condor_attributes = [(opt.replace('batchsubmit.condorbase.condor_attributes.',''),qcl.generic_get(self.apfqname, opt)) \
                                            for opt in qcl.options(self.apfqname) \
                                            if opt.startswith('batchsubmit.condorbase.condor_attributes.')]  # Note the . at the end of the pattern !!

            self.baselogdir = self.fcl.generic_get('Factory', 'baseLogDir') 
            self.baselogdirurl = self.fcl.generic_get('Factory', 'baseLogDirUrl') 

           
            condor.checkCondor()
            self.log.info(': Object properly initialized.')
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise


    def submit(self, n):
        '''
        n is the number of pilots to be submitted 
        Returns processed list of JobInfo objects. 
        
        '''
        self.log.debug('Preparing to submit %s jobs' %n)
        joblist = None

        if not utils.checkDaemon('condor'):
            self.log.debug('condor daemon is not running. Doing nothing')
            return joblist
        
        try:
            if n > 0:
                self._calculateDateDir()
                self.JSD = jsd.JSDFile()
                #self._getX509Proxy()
                self._addJSD()
                self._custom_attrs()
                self._finishJSD(n)
                jsdfile = self._writeJSD()
                if jsdfile:
                    #st, output = self.__submit(n, jsdfile)
                    # FIXME:
                    # factory, wmsqueue and submitargs should not be necessary
                    st, output = mynewsubmit(n, jsdfile, self.factory, self.wmsqueue, self.submitargs)
                    self.log.trace('Got output (%s, %s).' %(st, output)) 
                    joblist = condor.parsecondorsubmit(output)
                else:
                    self.log.debug('jsdfile has no value. Doing nothing')
            elif n < 0:
                # For certain plugins, this means to retire or terminate nodes...
                self.log.trace('Preparing to retire %s jobs' % abs(n))
                self.retire(abs(n))
            else:
                self.log.debug("Asked to submit 0. Doing nothing...")
            
            self.log.trace('Done. Returning joblist %s.' %joblist)
                
        except Exception, e:
            self.log.error('Exception during submit processing. Exception: %s' % e)
            self.log.error("Exception: %s" % traceback.format_exc())
        # we return the joblist so it can be sent to the monitor
        return joblist
        

    def retire(self, num):
        '''
         Do nothing by default. 
        '''
        self.log.trace('Default retire() do nothing.')


    def cleanup(self):
        '''
        '''
        self.log.info("Cleanup called. Noop.")
  
    
    def _calculateDateDir(self):
        '''
        a new directory is created for each day. 
        Sets logDir and logUrl
        Here we calculate it.
        '''
        now = time.gmtime() # gmtime() is like localtime() but in UTC
        timePath = "/%04d-%02d-%02d/" % (now[0], now[1], now[2])
        logPath = timePath + self.apfqname.translate(string.maketrans('/:','__'))
        self.logDir = self.baselogdir + logPath
        self.logUrl = self.baselogdirurl + logPath


 
    def _addJSD(self):

        self.log.trace('addJSD: Starting.')

        self.JSD.add("Dir", "%s/" % self.logDir)
        self.JSD.add("notify_user", "%s" % self.factoryadminemail)

        # -- MATCH_APF_QUEUE --
        # this token is very important, since it will be used by other plugins
        # to identify this pilot from others when running condor_q
        self.JSD.add('+MATCH_APF_QUEUE', '"%s"' % self.apfqname)

        ### Environment
        environment = '"PANDA_JSID=%s' % self.factoryid
        environment += ' GTAG=%s/$(Cluster).$(Process).out' % self.logUrl
        environment += ' APFCID=$(Cluster).$(Process)'
        environment += ' APFFID=%s' % self.factoryid
        if self.monitorurl:
            environment += ' APFMON=%s' % self.monitorurl
        environment += ' FACTORYQUEUE=%s' % self.apfqname
        if self.factoryuser:
            environment += ' FACTORYUSER=%s' % self.factoryuser
        if self.environ:
            if self.environ != 'None' and self.environ != '':
                    environment += " " + self.environ
        environment += '"'
        self.JSD.add('environment', environment)

       
        self.JSD.add("executable", "%s" % self.executable)
        if self.arguments:
            self.JSD.add('arguments', '%s' % self.arguments)

        # -- fixed stuffs -- 
        self.JSD.add("output", "$(Dir)/$(Cluster).$(Process).out")
        self.JSD.add("error", "$(Dir)/$(Cluster).$(Process).err")
        self.JSD.add("log", "$(Dir)/$(Cluster).$(Process).log")
        self.JSD.add("stream_output", "False")
        self.JSD.add("stream_error", "False")
        self.JSD.add("notification", "Error")
        self.JSD.add("transfer_executable", "True")
        
        self.log.trace('addJSD: Leaving.')
   
    def __parse_condor_attribute(self, s):
        '''
        auxiliar method to help splitting the string
        using the comma as splitting character.
        The trick here is what to do when the comma is preceded 
        by one or more \
        Sometimes the user wants the comma to be taken literally 
        instead of as an splitting char. In that case, the comma
        can be escaped with a \.
        And the \ can be escaped with another \ in case the user
        wants the \ to be literal. 
        '''
        p = re.compile(r"(\\)+,")  # regex matching for 1 or more \ followed by a ,
                                   # the backslash appears twice, but it means a single \
        m = re.finditer(p, s)      # searching for all ocurrencies 

        # now we create a list of pairs (x,y) where 
        #   x is the index of the first char matching the regexp: the first \ in our case.
        #   y is the index of the last char matching the regexp: the , in our case
        l = [(i.start(), i.end()) for i in m] 
       
        # we reverse the list, to start processing it from the end to the beginning.
        # In this way, each manipulation will not change the rest of indexes. 
        l.reverse()
        for i in l:
            nb_slashes = i[1] - i[0] - 1
            nb_real_slashes = nb_slashes / 2
            # each pair \\ actually has to be translated as \
        
            if nb_slashes % 2 == 0:
                # even nb of slashes
                # => nb/2 real slashes, and comma is splitting char
                s = s[:i[0]] + '\\'* nb_real_slashes + "," + s[i[1]:]
            else:
                # odd nb of slashes
                # => (nb-1)/2 real slashes, and comma is literal 
                s = s[:i[0]] + '\\'* nb_real_slashes + "APF_LITERAL_COMMA" + s[i[1]:]
        
        fields = []
        for field in s.split(','):
                # we change back the fake string APF_LITERAL_COMMA by an actual ,
                field = field.replace('APF_LITERAL_COMMA', ',')
                fields.append(field)
        
        return fields

 

    def _custom_attrs(self):
        ''' 
        adding custom attributes from the queues.conf file
        ''' 
        self.log.trace('Starting.')

        if self.condor_attributes:
            for attr in self.__parse_condor_attribute(self.condor_attributes):
                if '=' in attr:
                    #key = attr.split('=')[0]
                    #value = '='.join( attr.split('=')[1:] )
                    key, value = attr.split('=', 1)
                    self.JSD.add(key, value)
                else:
                    # I think this will never happens
                    self.JSD.add(attr)

        for item in self.extra_condor_attributes:
            self.JSD.add(item[0], item[1])

        self.log.trace('Leaving.')


    def _finishJSD(self, n):
        '''
        add the number of pilots (n)
        '''
        self.log.trace('finishJSD: Starting.')
        self.log.trace('finishJSD: adding queue line with %d jobs' %n)
        self.JSD.add("queue %d" %n)
        self.log.trace('finishJSD: Leaving.')

    def _writeJSD(self):
        '''
        Dumps the whole content of the JSDFile object into a disk file
        '''
    
        self.log.trace('writeJSD: Starting.')
        self.log.trace('writeJSD: the submit file content is\n %s ' %self.JSD)
        out = self.JSD.write(self.logDir, 'submit.jdl')
        self.log.trace('writeJSD: Leaving.')
        return out
