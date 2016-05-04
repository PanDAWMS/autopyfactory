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

from autopyfactory.factory import BatchSubmitInterface
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

        self.ec2_access_key = self.apfqueue.fcl.generic_get('Factory', 'batchstatus.euca.ec2_access_key', logger=self.log)
        self.ec2_secret_key = self.apfqueue.fcl.generic_get('Factory', 'batchstatus.euca.ec2_secret_key', logger=self.log)

        self.log.info('BatchSubmitPlugin: Object initialized.')

    def valid(self):
        return self._valid

    def submit(self, n):
        # for the time being, we assume the image is created
        # so we only run command euca-run-instances

        cmd = "euca-run-instances -n %s -a %s -s %s %s" %(n, self.ec2_access_key, self.ec2_secret_key, self.apfqname)
        (exitStatus, output) = commands.getstatusoutput(cmd)
        if exitStatus != 0:
            self.log.error('__submit: euca-run-instances command failed (status %d): %s', exitStatus, output)
        else:
            self.log.info('__submit: euca-run-instances command succeeded')
        st, out = exitStatus, output




###     def submit(self, n):
###         '''
###         n is the number of pilots to be submitted 
###         '''
### 
###         self.log.debug('submit: Preparing to submit %s pilots' %n)
### 
###         if n != 0:
### 
###             # FIXME !! Temporary solution
###             self.JSD = """name: 
### summary: Base SL 6 using internet-accessible repos, with BNL_CLOUD config
### version: 1
### release: 0
### os:
###   name: sl
###   version: 6
###   password: griddev
### hardware:
###   cpus: 1
###   memory: 2048
###   partitions:
###     "/":
###       size: 12
###       
### appliances:
###   - sl6-x86_64-base
###       
### packages:
###   - bind-utils
###   - condor
###   - cvmfs
###   - dhclient
###   - hepix-context
###   - osg-ca-certs
###   - osg-wn-client
###   - puppet
###   - subversion
###   - vim-enhanced
###   - wget
###   - yum
###   - yum-priorities
###   
### default_repos: false # default is true
### 
### repos:
###   - name: "racf-grid-devel"
###     baseurl: "http://dev.racf.bnl.gov/yum/grid/development/rhel/6Workstation/x86_64"
###     ephemeral: true
### 
### #  - name: "osg-repo"
### #    baseurl: "file:///home/src/repo/sl6-i386-osg-repo"
### #    ephemeral: true
### 
### #  - name: "osg-repo"
### #    baseurl: "http://repo.grid.iu.edu/3.0/el6/osg-release/x86_64"
### #    ephemeral: true
### 
###   - name: "osg-release-x86_64"
###     baseurl: "http://gcemaster01.rcf.bnl.gov/cobbler/repo_mirror/osg-release-x86_64-rhel6"
###     ephemeral: true
### 
###   - name: "cvmfs"
###     baseurl: "http://cvmrepo.web.cern.ch/cvmrepo/yum/cvmfs/EL/6.2/x86_64"
###     ephemeral: true
### 
###         
### files:
### 
###   "/etc/sysconfig":
###     - "selinux"
###   "/etc/rc.d":
###     - "rc.local"      
### 
###   "/etc/condor":
###     - "condor_config.local"
###   "/usr/libexec":
###     - "jobwrapper.sh"
### 
###   "/etc/sysconfig/modules":
###     - "fuse.modules"
###   "/etc/cvmfs":
###     - "default.local" 
###   "/etc/cvmfs/domain.d":
###     - "cern.ch.local"
###   "/etc":
###     - "auto.master"
###     - "fuse.conf"
###     
### post:
###   base:
###     - "chmod +x /etc/rc.local"
### 
###     - "/sbin/chkconfig fetch-crl-boot on"
###     - "/sbin/chkconfig fetch-crl-cron on"
### 
###     - "chmod +x /etc/sysconfig/modules/fuse.modules"
###     - "chmod +x /usr/libexec/jobwrapper.sh"
###     
###     - "mkdir /home/osg"
###     - "mkdir /home/osg/app"
###     - "chmod ugo+rwx /home/osg/app"    
###     - "mkdir /home/osg/data"
###     - "chmod ugo+rwx /home/osg/data"    
###        
###     - "/usr/sbin/useradd slot1"
###     - "/sbin/chkconfig condor on"
###     
###     - "/sbin/chkconfig cvmfs on" """ %self.apfqname
### 
###             st, output = self.__submit(n, jsdfile) 
###         else:
###             st, output = (None, None)
### 
###         self.log.debug('submit: Leaving with output (%s, %s).' %(st, output))
###         return st, output
### 
###     def _submit(n, jsdfile):
###         # FIXME !! Just a temporary solution
###         import tempfile
###         tmp = tempfile.mkstemp()[1]
###         appl_file = open(tmp, "w")
###         print >> appl_file, jsdfile
###         appl_file.close()
###         cmd = "boxgrinder-build %s -d openstack" %tmp
