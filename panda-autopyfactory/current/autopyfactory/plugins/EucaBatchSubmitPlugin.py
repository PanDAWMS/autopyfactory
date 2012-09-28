#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#


import commands
import logging
import os
import re
import string
import subprocess
import time

from autopyfactory.interfaces import BatchSubmitInterface
import autopyfactory.utils as utils
import jsd 

from persistent import *


__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class EucaBatchSubmitPlugin(BatchSubmitInterface):
    
    def __init__(self, apfqueue):

        self._valid = True
        self.log = logging.getLogger("main.batchsubmitplugin[%s]" %apfqueue.apfqname)

        self.apfqueue = apfqueue
        self.apfqname = apfqueue.apfqname
        self.factory = apfqueue.factory
        self.fcl = apfqueue.factory.fcl
        self.qcl = apfqueue.qcl
        self.condorpool = self.apfqueue.qcl.generic_get(self.apfqname, 'batchstatus.euca.condorpool', 'get', logger=self.log)
        
        # We need to know which APFQueue originally launched 
        # each VM. 
        # That info is recorded in a DB. 
        # We need to query that DB. 
        self._queryDB()

        self.log.info('BatchSubmitPlugin: Object initialized.')

    def valid(self):
        return self._valid

    def submit(self, n):

        self.log.debug('submit: Starting with n=%s' %n)
        if n>0:
            self._submit(n)
        if n<0:
            self.log.debug('n is less than 0, killing VMs instead of launching new ones')
            self._kill(-n)

        # after finishing everything the DB session has to be saved 
        self.persistencedb.save()
       
        self.log.debug('submit: Leaving.')

    def _submit(self, n):
        '''
        For the time being, we assume the image is created
        so we only run command euca-run-instances

        Output of euca-run-instances looks like:

            $euca-run-instances -t m1.small -n 3 ami-00000016 --conf /home/jhover/nova-essex/novarc
            RESERVATION    r-m0bg7090    c8d55513d64243fa8e0b29384f6f0c81    default
            INSTANCE  i-0000022d ami-00000016  server-557  server-557 pending None (c8d55513d64243fa8e0b29384f6f0c81, ct37.usatlas.bnl.gov)  0  m1.small 2012-09-20T19:31:42.000Z nova
            INSTANCE  i-0000022e ami-00000016  server-558  server-558 pending None (c8d55513d64243fa8e0b29384f6f0c81, ct42.usatlas.bnl.gov)  1  m1.small 2012-09-20T19:31:42.000Z nova
            INSTANCE  i-0000022f ami-00000016  server-559  server-559 pending None (c8d55513d64243fa8e0b29384f6f0c81, ct11.usatlas.bnl.gov)  2  m1.small 2012-09-20T19:31:42.000Z nova
        '''
        self.log.debug('_submit: Starting with n=%s' %n)

        cmd = "euca-run-instances -n %s --config %s %s" %(n, self.rcfile, self.executable)
        (exitStatus, output) = commands.getstatusoutput(cmd)
        if exitStatus != 0:
            self.log.error('__submit: euca-run-instances command failed (status %d): %s', exitStatus, output)
        else:
            self.log.info('__submit: euca-run-instances command succeeded')
        st, out = exitStatus, output

        # parse the output after submitting
        list_vm = []
        for line in out.split('\n'):
            fields = line.split()
            if fields[0] == 'INSTANCE':
                vm_instance = fields[1]
                host_name = fields[3]
                list_vm.append( (vm_instance, host_name) )
        
        self._addDB(list_vm)
        self.log.debug('_submit: Leaving')

    def _addDB(self, list_vm):
        '''
        ancilla method to add new entries to the DB
        so later on we will know it was this APFQueue
        the one who launched these VM instances

        list_vm is a list of pairs (vm_instance, host_name)
        '''

        self.log.debug('_addDB: Starting')
        from persistent import *
        
        o = PersistenceDB(self.fcl), VMInstance)
        o.createsession()

        instances = []
        for vm in list_vm:
            vm_instance = vm[0]
            host_name = vm[1]
            instances.append( VMInstance(apfqname=self.apfqname, vm_instance=vm_instance, host_name=host_name ) ) 

        o.add_all(instances)
        o.save()
        self.log.debug('_addDB: Leaving')

    def _kill(self, n):
        '''
        when the input n to submit() is negative, 
        this plugin interprets it as the number of
        NOTE: in _delete() is again >0.

        VM instances to be killed has to be chosen 
        among those with startd active. 
        A condor_off order will be sent. 

        The algorithm must take into account 
        VM with startd in status 'Retiring' 
        (<=> batchqueueinfo status 'done')
        will not run any more jobs, 
        so the condor_off order has to be sent to
        VMs with startd 'Busy'
        (<=> batchqueueinfo in status 'running')

        '''

        self.log.debug('_kill: Starting with n=%s' %n)
        self._stop_startd(n)
        self._stop_vm()
        self.log.debug('_kill: Leaving')

    def _stop_startd(self, n):
        '''
        stops n startds. 
        We do it using command condor_off. 
        There are two ways:
            $condor_off -peaceful -pool gridtest03.racf.bnl.gov:29660 -addr 10.0.0.11
            $condor_off -peaceful -pool gridtest03.racf.bnl.gov:29660 -name server-465
        '''
        # ---------------------------------------------------------
        #
        #   FIXME
        #
        #       - This is just a 1st approach.
        #         We just pick up the n first startd's that are Busy
        #         The final decission should take into account
        #         some time values (like for how long each one has been running)
        # ---------------------------------------------------------

        self.log.debug('_stop_startd: Starting with n=%s' %n)
        
        running_startd = self._running_startd() 
        for host in running_startd[:n]: # we pick up (TEMPORARY SOLUTION) the first n
            cmd = 'condor_off -peaceful -pool %s -name %s' %(self.condorpool, host)

        self.log.debug('_stop_startd: Leaving')



    #  -------------------------------------------------------------
    #
    #       FIXME
    #
    #   Maybe all of this can be done in the Euca BatchStatus Plugin
    #   since I am running here again (!) condor_status
    #
    #  -------------------------------------------------------------

    def _stop_vm(self):
        '''
        Terminates all VMs with no startd running.
        Command to terminate a VM looks like:

            $ euca-terminate-instances i-0000022e i-0000022f --conf /home/jhover/nova-essex/novarc

        '''
        self.log.debug('_stop_vm: Starting')

        db_hosts = self._queryDB_hosts()
        list_condor_hosts = self._condor_hosts()

        for host, vm_instance in db_hosts.iteritems():
            if not self._host_in_condor(host, list_condor_hosts):
                self._terminate_instance( vm_instance )

        self.log.debug('_stop_vm: Leaving')



    def _queryDB(self):
        '''
        ancilla method to query the DB to find out
        which APFQueue launched each VM instance
        It returns a dictionary:
            keys are the vm instances
            values are the APFQueue names
        '''

        self.log.debug('_queryDB: Starting')

        self.persistencedb = PersistenceDB(self.apfqueue.fcl), VMInstance)
        self.persistencedb.createsession()

        self.list_vm = self.persistencedb.query()

        self.dict_vm_apfqname = {}
        for i in self.list_vm:
            self.dict_vm_apfqname[i.host_name] = i.apfqname

        self.log.debug('_queryDB: Leaving')



    def _queryDB_hosts(self):
        '''
        ancilla method to query the DB to find out
        which APFQueue launched each VM instance
        It returns a dictionary for this particular APFQueue:
            - keys are the host name
            - values are the vm instance
        '''

        self.log.debug('_queryDB: Starting')


        o = PersistenceDB(self.apfqueue.fcl), VMInstance)
        o.createsession()


        self.log.debug('_queryDB: Leaving with dict %s' %dict_hosts)
        return list_hosts

    def _condor_hosts(self):
        '''
        runs condor_status to get the list of hostnames
        '''
        # -----------------------------------------------------
        # FIXME
        #   I am running condor_status again!!!
        # -----------------------------------------------------

        list_hosts = []
        querycmd = 'condor_status --pool %s -format "Name=%s\n" Name' % self.condorpool
        p = subprocess.Popen(querycmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (out, err) = p.communicate()
        for line in output.split('\n'):
           host = line.split('=')[1]
           list_hosts.append(host) 
        return list_hosts


    def self._host_in_condor(self, host, list_condor_hosts):
        '''
        checks if host is in the list.
        
        host looks like server-557
        items in list_condor_hosts look like  server-456.novalocal
        '''

        self.log.debug('_host_in_db: Starting for host=%s' %host)

        out = False # default value
        for i in list_condor_hosts:
            if i.startswith(host):
                out = True
                break 

        self.log.debug('_host_in_db: Leaving with output=%s' %out)
        return out


    def _running_startd(self): 
        # -----------------------------------------------------
        # FIXME
        #   I am running condor_status again!!!
        # -----------------------------------------------------
        
        list_hosts = []
        querycmd = 'condor_status --pool %s -format "Name=%s " Name -format "Activity=%s\n" Activity' % self.condorpool
        p = subprocess.Popen(querycmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (out, err) = p.communicate()
        for line in output.split('\n'):
            host = line.split()[0].split('=')[1] 
            activity = line.split()[1].split('=')[1] 
            if activity in ['Busy' , 'Idle']:
                    list_hosts.append( line ) 
        return list_hosts
        


    def _terminate_instance(self, vm_instance):
        '''
        terminates a single instance
        '''
        # -----------------------------------------------------
        # FIXME
        #   - maybe is more efficient to terminate a list of instances at once
        #   - so far, the conf file is hardcoded
        #   - so far, the remote host is hardcoded
        # -----------------------------------------------------


        self.log.debug('_terminate_instance: Starting for vm_instance=%s' %vm_instance)
        cmd = 'ssh gridreserve30.usatlas.bnl.gov "euca-terminate-instances %s --conf /home/jhover/nova-essex/novarc"' %vm_instance
        self.log.info('_terminate_instance: cmd is %s' %cmd)
        commands.getoutput(cmd)
        self.log.debug('_terminate_instance: Leaving')

