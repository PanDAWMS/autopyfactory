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
from autopyfactory import jsd

from autopyfactory.persistence import *


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

        self.log = logging.getLogger("main.batchsubmitplugin[%s]" %apfqueue.apfqname)

        self.apfqueue = apfqueue
        self.apfqname = apfqueue.apfqname
        self.factory = apfqueue.factory
        self.fcl = apfqueue.factory.fcl
        self.qcl = apfqueue.qcl
        self.condorpool = self.apfqueue.qcl.generic_get(self.apfqname, 'batchstatus.euca.condorpool', 'get', logger=self.log)
        
        # We need to know which APFQueue originally launched each VM. 
        # That info is recorded in a DB. 
        # We need to query that DB. 
        self.persistencedb = PersistenceDB(self.apfqueue.fcl, VMInstance)

        self.log.info('BatchSubmitPlugin: Object initialized.')

    def submit(self, n):

        # ------------------------------------------
        #  FIXME
        #   this method is supposed to return something
        #   to be passed to the monitor
        # ------------------------------------------


        self.log.debug('submit: Starting with n=%s' %n)
        if n>0:
            self._submit(n)
        if n<0:
            self.log.debug('n is less than 0, killing VMs instead of launching new ones')
            self._kill(-n)
        
        # now we need to terminate all VM with no startd active
        self._purge()

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

        #  --------------------------------------------
        #   FIXME
        #       for the time being, some values are hardcoded
        #  --------------------------------------------
        cmd = 'ssh root@gridreserve30.usatlas.bnl.gov "euca-run-instances -n %s --config /home/jhover/nova-essex/novarc"' %n
        (exitStatus, output) = commands.getstatusoutput(cmd)
        if exitStatus != 0:
            self.log.error('__submit: euca-run-instances command failed (status %d): %s', exitStatus, output)
        else:
            self.log.info('__submit: euca-run-instances command succeeded')
        st, out = exitStatus, output

        # parse the output after submitting
        list_new_vm = []
        for line in out.split('\n'):
            fields = line.split()
            if fields[0] == 'INSTANCE':
                vm_instance = fields[1]
                host_name = fields[3]
                list_new_vm.append( (vm_instance, host_name) )
        
        self._addDB(list_new_vm)
        self.log.debug('_submit: Leaving')


    def _addDB(self, list_new_vm):
        '''
        ancilla method to add new entries to the DB
        so later on we will know it was this APFQueue
        the one who launched these VM instances

        list_new_vm is a list of pairs (vm_instance, host_name)
        '''

        self.log.debug('_addDB: Starting')

        new_instances = []
        for vm in list_new_vm:
            vm_instance = vm[0]
            host_name = vm[1]
            new_instances.append( VMInstance(apfqname=self.apfqname, vm_instance=vm_instance, host_name=host_name ) ) 

        self.persistencedb.session.add_all(new_instances)
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
        self.log.debug('_kill: Leaving')

    def _stop_startd(self, n):
        '''
        stops n startds. 
        We do it using command condor_off. 
        There are two ways:
            $condor_off -peaceful -pool gridtest03.racf.bnl.gov:29660 -addr 10.0.0.11
            $condor_off -peaceful -pool gridtest03.racf.bnl.gov:29660 -name server-465.novalocal
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
        
        i = 0
        for vm in self.persistencedb.list_vm:
            if vm.startd_status in ['Busy', 'Idle']:
                #  --------------------------------------------
                #   FIXME
                #       for the time being, some values are hardcoded
                #  --------------------------------------------
                #cmd = 'ssh root@grid13.racf.bnl.gov "condor_off -peaceful -pool %s -name %s"' %(self.condorpool, vm.condor_host_name)
                cmd = 'ssh root@grid13.racf.bnl.gov "condor_off -peaceful -pool gridtest03.racf.bnl.gov:29660 -name %s"' %( vm.condor_host_name)
                self.log.info('_stop_startd: stopping startd with cmd = %s' %cmd)
                commands.getoutput(cmd)
                i += 1
                if i == n:
                    break

        self.log.debug('_stop_startd: Leaving')


    def _purge(self):
        '''
        Terminates all VMs with no startd running.
        They appear in the DB with startd_status = None (None as string)
        Command to terminate a VM looks like:

            $ euca-terminate-instances i-0000022e i-0000022f --conf /home/jhover/nova-essex/novarc

        '''
        self.log.debug('_purge: Starting')

        for vm in self.persistencedb.list_vm:
            if vm.startd_status == 'None':
                self.log.info('_purge: vm % has no startd active.' %vm.vm_instance)
                self._terminate_instance(vm)

        self.log.debug('_purge: Leaving')


    def _terminate_instance(self, vm):
        '''
        terminates a single instance
            - use command euca-terminate-instances to terminate the instance
            - remove the entry from the session
        '''
        self.log.debug('_terminate_instance: Starting for vm_instance=%s' %vm_instance)
        self._kill_instance(vm)
        self._delete_instance(vm)
        self.log.debug('_terminate_instance: Leaving')


    def _kill_instance(self, vm):
        '''
        run euca command to terminate a given instance
        vm is one of the object from the self.list_vm (class VMInstance)
        '''

        # -----------------------------------------------------
        # FIXME
        #   - maybe is more efficient to terminate a list of instances at once
        #   - so far, the conf file is hardcoded
        #   - so far, the remote host is hardcoded
        # -----------------------------------------------------

        self.log.debug('_kill_instance: Starting for instance %s' % vm.vm_instance)

        cmd = 'ssh gridreserve30.usatlas.bnl.gov "euca-terminate-instances %s --conf /home/jhover/nova-essex/novarc"' %vm.vm_instance
        self.log.info('_kill_instance: cmd is %s' %cmd)
        commands.getoutput(cmd)

        self.log.debug('_kill_instance: Leaving')


    def _delete_instance(self, vm):
        '''
        vm is one of the object from the self.list_vm (class VMInstance)
        Delete it from the list
        '''

        self.log.debug('_delete_instance: Starting for instance %s' % vm.vm_instance)
        self.persistencedb.session.delete(vm)
        self.log.debug('_delete_instance: Leaving')






    ### # ----------------------------------------------------
    ### #   FIXME
    ### #       this code is repeated in Euca Status Plugin
    ### #       maybe it should be in persistent.py
    ### # ----------------------------------------------------
    ### def _queryDB(self):
    ###     '''
    ###     ancilla method to query the DB to find out
    ###     which APFQueue launched each VM instance
    ###     It creates a list of Instance objects
    ###     '''
    ###     self.log.debug('_queryDB: Starting')
    ###     self.persistencedb = PersistenceDB(self.apfqueue.fcl), VMInstance)
    ###     self.list_vm = self.persistencedb.query()
    ###     self.log.debug('_queryDB: Leaving')

    ### def _queryDB_hosts(self):
    ###     '''
    ###     ancilla method to query the DB to find out
    ###     which APFQueue launched each VM instance
    ###     It returns a dictionary for this particular APFQueue:
    ###         - keys are the host name
    ###         - values are the vm instance
    ###     '''
    ###     self.log.debug('_queryDB: Starting')
    ###     o = PersistenceDB(self.apfqueue.fcl), VMInstance)
    ###     o.createsession()
    ###     self.log.debug('_queryDB: Leaving with dict %s' %dict_hosts)
    ###     return list_hosts

    ### def _condor_hosts(self):
    ###     '''
    ###     runs condor_status to get the list of hostnames
    ###     '''
    ###     # -----------------------------------------------------
    ###     # FIXME
    ###     #   I am running condor_status again!!!
    ###     # -----------------------------------------------------
    ###     list_hosts = []
    ###     querycmd = 'condor_status --pool %s -format "Name=%s\n" Name' % self.condorpool
    ###     p = subprocess.Popen(querycmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ###     (out, err) = p.communicate()
    ###     for line in output.split('\n'):
    ###        host = line.split('=')[1]
    ###        list_hosts.append(host) 
    ###     return list_hosts

    ### def _running_startd(self): 
    ###     # -----------------------------------------------------
    ###     # FIXME
    ###     #   I am running condor_status again!!!
    ###     # -----------------------------------------------------
    ###     
    ###     list_hosts = []
    ###     querycmd = 'condor_status --pool %s -format "Name=%s " Name -format "Activity=%s\n" Activity' % self.condorpool
    ###     p = subprocess.Popen(querycmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ###     (out, err) = p.communicate()
    ###     for line in output.split('\n'):
    ###         host = line.split()[0].split('=')[1] 
    ###         activity = line.split()[1].split('=')[1] 
    ###         if activity in ['Busy' , 'Idle']:
    ###                 list_hosts.append( line ) 
    ###     return list_hosts

    ### def self._host_in_condor(self, host, list_condor_hosts):
    ###     '''
    ###     checks if host is in the list.
    ###     host looks like server-557
    ###     items in list_condor_hosts look like  server-456.novalocal
    ###     '''
    ###     self.log.debug('_host_in_db: Starting for host=%s' %host)
    ###     out = False # default value
    ###     for i in list_condor_hosts:
    ###         if i.startswith(host):
    ###             out = True
    ###             break 
    ###     self.log.debug('_host_in_db: Leaving with output=%s' %out)
    ###     return out
