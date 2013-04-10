#!/bin/env python
#
# AutoPyfactory batch status plugin for OpenStack
#

import subprocess
import logging
import os
import time
import threading
import traceback
import xml.dom.minidom

from autopyfactory.interfaces import BatchStatusInterface
from autopyfactory.factory import BatchStatusInfo
from autopyfactory.factory import QueueInfo
from autopyfactory.factory import Singleton, CondorSingleton
from autopyfactory.info import InfoContainer
from autopyfactory.info import BatchStatusInfo

from persistent import *

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"


class EucaBatchStatusPlugin(threading.Thread, BatchStatusInterface):
    '''
    -----------------------------------------------------------------------
    This class is expected to have separate instances for each PandaQueue object. 
    The first time it is instantiated, 
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from Thread and from BatchStatusInterface
    -----------------------------------------------------------------------
    '''
    
    __metaclass__ = Singleton 
    #################################################################
    #
    #  FIXME:  We need a separate singleton per each local pool
    #
    #################################################################
    
    def __init__(self, apfqueue, **kw):

        try:
            threading.Thread.__init__(self) # init the thread
            
            self.log = logging.getLogger("main.batchstatusplugin[singleton created by %s]" %apfqueue.apfqname)
            self.log.debug('BatchStatusPlugin: Initializing object...')
            self.stopevent = threading.Event()

            # to avoid the thread to be started more than once
            self.__started = False

            self.apfqueue = apfqueue
            self.apfqname = apfqueue.apfqname
            self.sleeptime = self.apfqueue.fcl.generic_get('Factory', 'batchstatus.euca.sleep', 'getint', default_value=60, logger=self.log)
            self.condorpool = self.apfqueue.qcl.generic_get(self.apfqname, 'batchstatus.euca.condorpool', 'get', logger=self.log)
            self.currentinfo = None              

            # variable to record when was last time info was updated
            # the info is recorded as seconds since epoch
            self.lasttime = 0

            # We need to know which APFQueue originally launched each VM. 
            # That info is recorded in a DB. 
            # We need to query that DB. 
            self.persistencedb = PersistenceDB(self.apfqueue.fcl, VMInstance)

            self.log.info('BatchStatusPlugin: Object initialized.')
        except Exception, ex:
            self.log.error("BatchStatusPlugin object initialization failed. Raising exception")
            raise ex

    def getInfo(self, maxtime=0):
        '''
        Returns a BatchStatusInfo object populated by the analysis 
        over the output of a condor_status command

        Optionally, a maxtime parameter can be passed.
        In that case, if the info recorded is older than that maxtime,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        '''           
        self.log.debug('getInfo: Starting with maxtime=%s' % maxtime)
        
        if self.currentinfo is None:
            self.log.debug('getInfo: Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentinfo.lasttime) > maxtime:
            self.log.debug('getInfo: Info too old. Leaving and returning None.')
            return None
        else:                    
            self.log.debug('getInfo: Leaving and returning info of %d entries.' % len(self.currentinfo))
            return self.currentinfo


    def start(self):
        '''
        We override method start() to prevent the thread
        to be started more than once
        '''

        self.log.debug('start: Starting')

        if not self.__started:
                self.log.debug("Creating Condor batch status thread...")
                self.__started = True
                threading.Thread.start(self)

        self.log.debug('start: Leaving.')

    def run(self):
        '''
        Main loop
        '''

        self.log.debug('run: Starting')
        while not self.stopevent.isSet():
            try:
                self._update()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
            self.log.debug("Sleeping for %d seconds..." % self.sleeptime)
            time.sleep(self.sleeptime)
        self.log.debug('run: Leaving')

    def _update(self):
        '''        
        '''

        self.log.debug('_update: Starting.')
       
        try:
            strout = self._query()
            if not strout:
                self.log.warning('_update: output of query is not valid. Not parsing it. Skip to next loop.') 
            else:

                # update the session in the DB
                self._updateDB(strout)

                newinfo = self._parseoutput(strout)
                self.log.info("Replacing old info with newly generated info.")
                self.currentinfo = newinfo

        except Exception, e:
            self.log.error("_update: Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())            

        # close the DB session
        self.persistencedb.save()

        self.log.debug('__update: Leaving.')

    def _query(self):
        '''
        query command is like
            $ condor_status -pool <centralmanagerhostname[:portnumber]> 
        or, in XML format:
            $ condor_status -pool <centralmanagerhostname[:portnumber]> -format "%s" Name -format "%s" Activity -format "%s" State -xml

        Goal is to know how many startd's are running/retiring (or State=Claimed)
        and how many are idle (or State=Owner) 
        '''

        self.log.debug('_query: Starting.')

        #################################################################
        #
        #  FIXME:  We need a separate singleton per each local pool
        #
        #################################################################

        # -------------------------------------------
        # this is a temporary solution:
        #       we dont use yet XML, but raw data instead
        # -------------------------------------------
        #querycmd = 'condor_status -pool %s -format "Name=%s " Name -format "Activity=%s " Activity -format "State=%s " State -format "IP=%s\n" MyAddress' % self.condorpool
        querycmd = 'condor_status -pool gridtest03.racf.bnl.gov:29660 -format "Name=%s " Name -format "Activity=%s " Activity -format "State=%s " State -format "IP=%s\n" MyAddress' 

        # Note:
        #   There will be VMs with no startd active. 
        #   That is because in a previous cycle, the startd was order to stop.
        #   We can ignore them at this point, 
        #   since what is relevant is the number of active startd.
        #   The submit plugin will purge these empty VMs.


        self.log.debug('_query: Querying cmd = %s' %querycmd.replace('\n','\\n'))

        before = time.time()          
        p = subprocess.Popen(querycmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)     
        out = None
        (out, err) = p.communicate()
        delta = time.time() - before
        self.log.debug('_query: it took %s seconds to perform the query' %delta)
        if p.returncode == 0:
            self.log.debug('_queryk: Leaving with OK return code.') 
        else:
            self.log.warning('_query: Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
            out = None
        self.log.debug('_query: Leaving. Out is %s' % out)
        return out


    def _parseoutput(self, output):
        '''
        output looks like


          Name               OpSys      Arch   State     Activity LoadAv Mem   ActvtyTime

          server-486.novaloc LINUX      X86_64 Claimed   Busy     1.000  2010  0+17:32:22
          server-487.novaloc LINUX      X86_64 Claimed   Busy     1.230  2010  0+00:18:40
          server-501.novaloc LINUX      X86_64 Claimed   Busy     1.000  2010  0+14:56:47
          server-502.novaloc LINUX      X86_64 Owner     Idle     0.000  2010 46+12:58:44
          server-500.novaloc LINUX      X86_64 Claimed   Busy     1.040  2010  0+17:18:37


        or in XML format, something like


          <?xml version="1.0"?>
          <!DOCTYPE classads SYSTEM "classads.dtd">
          <classads>
          <c>
              <a n="MyType"><s>Machine</s></a>
              <a n="TargetType"><s>Job</s></a>
              <a n="Activity"><s>Busy</s></a>
              <a n="Name"><s>server-486.novalocal</s></a>
              <a n="CurrentTime"><e>time()</e></a>
              <a n="State"><s>Claimed</s></a>  
          </c>
          <c>
              <a n="MyType"><s>Machine</s></a>
              <a n="TargetType"><s>Job</s></a>
              <a n="Activity"><s>Idle</s></a>
              <a n="Name"><s>server-502.novalocal</s></a>
              <a n="CurrentTime"><e>time()</e></a>
              <a n="State"><s>Owner</s></a>  
          </c>
          </classads>

        or using -format:

            Name=server-486.novalocal Activity=Idle State=Unclaimed IP=<10.0.0.15:21533?CCBID=130.199.185.191:29660#126296&PrivNet=localdomain>
            Name=server-487.novalocal Activity=Busy State=Claimed IP=<10.0.0.19:23285?CCBID=130.199.185.191:29660#126683&PrivNet=localdomain>
            Name=server-488 Activity=Busy State=Claimed IP=<10.0.0.20:26498?CCBID=130.199.185.191:29660#164846&PrivNet=localdomain>
            Name=server-489.novalocal Activity=Busy State=Claimed IP=<10.0.0.22:28687?CCBID=130.199.185.191:29660#126617&PrivNet=localdomain>
            Name=server-490.novalocal Activity=Busy State=Claimed IP=<10.0.0.25:22993?CCBID=130.199.185.191:29660#126665&PrivNet=localdomain>
        '''

        self.log.debug('_parseoutput: Starting with data %s' %output)

        batchstatusinfo = InfoContainer('batch', BatchStatusInfo())


        # analyze output of condor_status command
        
        # -------------------------------------------
        # this is a temporary solution:
        #       we dont use yet XML, but raw data instead
        # -------------------------------------------

        for line in output.split('\n'):
            if line != '':
                fields = line.split()
                condor_host_name = fields[0].split('=')[1]
                activity = fields[1].split('=')[1]
                state = fields[2].split('=')[1]
                ip = fields[3].split('=')[1]  # not really... FIXME  

                apfqname = self._get_apfqname(condor_host_name)

                # There could be VMs not launched by APF.
                # Those will no be in the DB. 
                # We ignore them
                if apfqname:

                    if apfqname not in batchstatusinfo.keys():
                        batchstatusinfo[apfqname] = BatchStatusInfo()

                    if activity == 'Busy':
                        batchstatusinfo[apfqname].running += 1
                    if activity == 'Idle':
                        batchstatusinfo[apfqname].running += 1
                    if activity == 'Retiring':
                        batchstatusinfo[apfqname].done += 1

        self.log.debug('_parseoutput: Leaving')
        return batchstatusinfo




    # --------------------------------------------
    #  FIXME
    #   this is a temporary solution,
    #   we will need a better solution
    # --------------------------------------------
    def _updateDB(self, output):
        '''
        output is the output from condor_status
        '''

        self.log.debug('_upateDB: Starting')

       
        # ------------------------------------ 
        #       FIXME
        #       This double loop is very inefficient
        # ------------------------------------ 

        for vm in self.persistencedb.list_vm:
            for line in output.split('\n'):
                if line != '':
                    fields = line.split()
                    condor_host_name = fields[0].split('=')[1]  # looks like server-456.novalocal

                    activity = fields[1].split('=')[1]

                    if condor_host_name.startswith(vm.host_name): # vm.host_name looks like server-456
                        vm.startd_status = activity 

                        # if the condor_host_name column in the VM has no value,
                        # add it now
                        if vm.condor_host_name != condor_host_name:
                            vm.condor_host_name = condor_host_name

                        break
            else:
                # no hostname from condor_status is in the DB
                # That means that startd is gone 
                # The entry in DB has to be marked, so the VM can be killed
                vm.startd_status = 'None'


        self.log.debug('_upateDB: Leaving')


    # --------------------------------------------
    #  FIXME
    #   this is a temporary solution,
    #   we will need a better solution
    #
    #   We should consider using filter_by()
    #   and, if needed, like(): 
    #       session.query().filter_by(  var.like(...) )
    # --------------------------------------------
    def _get_apfqname(self, condor_host_name):
        '''
        check if host_name is one of the hosts in the DB
        If it is, return the apfqname for that entry
        '''

        self.log.debug('_get_apfqname: Starting with host_name=%' %host_name)

        out=None   # default output

        #for vm in self.persistencedb.list_vm:
        #    if host_name.startswith(vm.host_name):
        #        self.log.debug('_get_apfqname: entry in the DB with host_name=%s found' %host_name)
        #        out = vm.apfqname 

        for vm in self.persistencedb.list_vm:
            if vm.condor_host_name == condor_host_name:
                self.log.debug('_get_apfqname: entry in the DB with condor_host_name=%s found' %condor_host_name)
                out = vm.apfqname 
                break

        self.log.debug('_get_apfqname: Leaving with output=%' %out)
        return out 



    def join(self, timeout=None):
        ''' 
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        ''' 

        self.log.debug('join: Starting with input %s' %timeout)
        self.stopevent.set()
        self.log.debug('Stopping thread....')
        threading.Thread.join(self, timeout)
        self.log.debug('join: Leaving')



    ### # ----------------------------------------------------
    ### #   FIXME
    ### #       this code is repeated in Euca Submit Plugin
    ### #       maybe it should be in persistent.py
    ### # ----------------------------------------------------
    ### def _queryDB(self):
    ###     '''
    ###     ancilla method to query the DB 
    ###     It creates a list with Instance objects
    ###     '''
    ###     self.log.debug('_queryDB: Starting')
    ###     self.persistencedb = PersistenceDB(self.apfqueue.fcl), VMInstance)
    ###     self.log.debug('_queryDB: Leaving')
