#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#


import commands
import logging
import os
import re
import string
import time

from autopyfactory.interfaces import BatchSubmitInterface
import autopyfactory.utils as utils
import jsd 

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
        self.executable = qcl.generic_get(self.apfqname, 'executable', logger=self.log)

        self.log.info('BatchSubmitPlugin: Object initialized.')

    def valid(self):
        return self._valid

    def submit(self, n):

        if n>0:
            self._submit(n)
        if n<0:
            self._delete(-n)

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
            if line.startswith('INSTANCE'):
                fields = line.split()
                vm_instance = fields[1]
                host_name = fields[3]
                list_vm.append( (vm_instance, host_name) )
        
        self._addDB(list_vm)

    def _addDB(self, list_vm):
        '''
        ancilla method to add new entries to the DB
        so later on we will know it was this APFQueue
        the one who launched these VM instances

        list_vm is a list of pairs (vm_instance, host_name)
        '''

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



    def _delete(self, n):
        '''
        when the in put n to submit() is negative, 
        this plugin interprets it as the number of
        VM instances to be killed.
            - the first candidates are those
              where the startd is 'Idle'
            - after that, some VMs still running
              will get a condor_off order.
        '''
