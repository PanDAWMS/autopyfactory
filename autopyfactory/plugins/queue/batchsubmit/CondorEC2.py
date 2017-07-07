#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorBase import CondorBase
from autopyfactory import jsd 
from autopyfactory.condor import killids, mincondorversion
import subprocess
import time
import commands
import logging
import traceback

#mincondorversion(8,1,1)


class CondorEC2(CondorBase):
    id = 'condorec2'
    
    def __init__(self, apfqueue, config, section):

        qcl = config
        
        newqcl = qcl.clone().filterkeys('batchsubmit.condorec2', 'batchsubmit.condorbase')
        super(CondorEC2, self).__init__(apfqueue, newqcl, section)
       
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.gridresource') 
            self.ami_id = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.ami_id')
            self.instance_type  = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.instance_type')
            self.user_data = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.user_data')
            self.user_data_file = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.user_data_file')
            self.access_key_id = qcl.generic_get(self.apfqname,'batchsubmit.condorec2.access_key_id')
            self.secret_access_key = qcl.generic_get(self.apfqname,'batchsubmit.condorec2.secret_access_key')
            self.spot_price = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.spot_price')
            self.usessh = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.usessh')
            if self.spot_price:                
                self.spot_price = float(self.spot_price)
            self.peaceful = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.peaceful')
            self.security_groups = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.security_groups')
            self.log.debug("Successfully got all config values for EC2BatchSubmit plugin.")
            self.log.debug('CondorEC2: Object properly initialized.')
        except Exception, e:
            self.log.error("Problem getting object configuration variables.")
            self.log.debug("Exception: %s" % traceback.format_exc())

               
    def _addJSD(self):
        """
        add things to the JSD object
        """

        self.log.debug('CondorEC2.addJSD: Starting.')
        super(CondorEC2, self)._addJSD()
        
        self.JSD.add("universe", "grid")
        self.JSD.add('grid_resource', 'ec2 %s' % self.gridresource) 

        self.JSD.add("ec2_access_key_id", "%s" % self.access_key_id) 
        self.JSD.add("ec2_secret_access_key", "%s" % self.secret_access_key) 

        # -- EC2 specific parameters --
        self.JSD.add("ec2_ami_id", "%s" % self.ami_id) 
        self.JSD.add("executable", "%s" % self.apfqueue.apfqname)
        self.JSD.add("ec2_instance_type", "%s" % self.instance_type) 
        if self.user_data:
            self.JSD.add('ec2_user_data', '%s' % self.user_data)
        if self.user_data_file:
            self.JSD.add('ec2_user_data_file', '%s' % self.user_data_file)      
        if self.spot_price:
            self.JSD.add('ec2_spot_price', '%f' % self.spot_price)
        if self.security_groups:
            self.JSD.add('ec2_security_groups', '%s' % self.security_groups)

        self.log.debug('CondorEC2.addJSD: Leaving.')


    def retire(self, n, order='oldest'):
        """
        trigger retirement of this many nodes, by looking at this parent APF queue's 
        CondorCloudBatchStatus plugin. 
        
        Scan jobinfo for node start times
           execute condor_off -peaceful -daemon startd -name <machine>
        OR
           ssh <EC2PublicIP> condor_off -peaceful -daemon startd
                
        for each desired retirement. 
        
        We only retire by shutting down startds, never by terminating VM jobs directly. This is to
        avoid race conditions/slight information mismatches due to time. E.g. just because the last condor_status 
        showed a startd as idle, doesn't mean it is still idle during this queue cycle. 
       
        But we should preferentially retire nodes that are Idle over ones that we know are busy.
        
        If we don't have information about startds, then unconditionally kill N VM jobs for this queue. 
                         
        """
        self.log.debug("Beginning to retire %d VM jobs..." % n)
        self.log.debug("Getting jobinfo for [%s]" % (self.apfqueue.apfqname))
        jobinfo = self.apfqueue.batchstatus_plugin.getJobInfo(queue=self.apfqueue.apfqname)
        self.log.debug("Jobinfo is %s" % jobinfo)
        if jobinfo:
            if self.peaceful:
                numtoretire = n
                numretired = 0           
                idlelist = []
                busylist = []            
                for job in jobinfo:
                    if job.executeinfo is not None:
                        self.log.debug("Handling instanceid =  %s" % job.executeinfo.instanceid)              
                        stat = job.executeinfo.getStatus()
                        if stat == 'busy':
                            busylist.append(job)
                        elif stat == 'idle':
                            idlelist.append(job)
                sortedlist = idlelist + busylist

                for job in sortedlist:
                    self._retirenode(job)
                    numtoretire = numtoretire - 1
                    numretired += 1
                    self.log.debug("numtoretire = %d" % numtoretire)
                    if numtoretire <= 0:
                        break
                self.log.debug("Retired %d VM jobs" % numretired)
            
            else:
                self.log.debug("Non-peaceful. Kill VM jobs...")
                if order == 'oldest':
                    jobinfo.sort(key = lambda x: x.enteredcurrentstatus)
                elif order == 'newest':
                    jobinfo.sort(key = lambda x: x.enteredcurrentstatus, reverse=True)
                killist = []
                for i in range(0, n):
                    j = jobinfo.pop()
                    killlist.append( "%s.%s" % (j.clusterid, j.procid))
                self.log.debug("About to kill list of %s ids. First one is %s" % (len(killlist), killlist[0] ))
                killids(killlist)
                self.log.debug("killids returned.")
        else:
            self.log.debug("Some info unavailable. Do nothing.")


    
    def _retirenode(self, jobinfo):
        """
        Do whatever is needed to tell the node to retire...
        """
        self.log.debug("Retiring node %s (%s)" % (jobinfo.executeinfo.hostname, 
                                                 jobinfo.ec2instancename))
        exeinfo = jobinfo.executeinfo
        publicip = exeinfo.hostname
        machine = exeinfo.machine
        condorid = "%s.%s" % (jobinfo.clusterid, jobinfo.procid)
        
        if self.usessh:
            self.log.debug("Trying to use SSH to retire node %s" % publicip)
            if self.peaceful:
                cmd='ssh root@%s "condor_off -peaceful -startd"' % publicip
            else:
                cmd='ssh root@%s "condor_off -startd"' % publicip
            self.log.debug("retire cmd is %s" % cmd) 
            before = time.time()
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            out = None
            (out, err) = p.communicate()
            delta = time.time() - before
            self.log.debug('It took %s seconds to issue the command' %delta)
            self.log.debug('%s seconds to issue command' %delta)
            if p.returncode == 0:
                self.log.debug('Leaving with OK return code.')
            else:
                self.log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))          
            # invoke ssh to retire node
        else:
            # call condor_off locally
            self.log.debug("Trying local retirement of node %s" % publicip)
            if machine.strip() != "":
                if self.peaceful:
                    cmd='condor_off -peaceful -startd -name %s' % machine
                else:
                    cmd='condor_off -startd -name %s' % machine
                self.log.debug("retire cmd is %s" % cmd) 
                before = time.time()
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                out = None
                (out, err) = p.communicate()
                delta = time.time() - before
                self.log.debug('It took %s seconds to issue the command' %delta)
                self.log.debug('%s seconds to issue command' %delta)
                if p.returncode == 0:
                    self.log.debug('Leaving with OK return code.')
                else:
                    out = out.replace("\n", " ")
                    out = err.replace("\n", " ")
                    self.log.warning('Leaving with bad return code. rc=%s err="%s" out="%s"' %(p.returncode, err, out ))          
            else:
                self.log.warning("Unable to retire node %s (%s) because it has an empty machine name." % (jobinfo.executeinfo.hostname,
                                                                                                          jobinfo.ec2instancename))
                
    def cleanup(self):
        """
        
        """
        self.log.debug("Cleanup called in EC2. Retiring...")
        self._killretired()

        
    def _killretired(self):
        """
        scan through jobinfo for this queue with job
        
        """
        self.log.debug("Killretired process triggered. Searching...")
        try:
            jobinfo = self.apfqueue.batchstatus_plugin.getJobInfo(queue=self.apfqueue.apfqname)
            self.log.debug("Finding and killing VM jobs in 'retired' state.")
        
            killlist = []
            if jobinfo:        
                for j in jobinfo:
                    self.log.debug("jobinfo is %s " % j)
                    if j.executeinfo:
                        st = j.executeinfo.getStatus()
                        self.log.debug("exe status for %s is %s" % (j.ec2instancename, st)  )
                        if st == 'retired':
                            killlist.append( "%s.%s" % (j.clusterid, j.procid))
                    else:
                        self.log.warning("There seems to be a VM job without even exeinfo. ec2id: %s" % j.ec2instancename)
                self.log.debug("killlist length is %s" % len(killlist))
            if killlist:
                self.log.debug("About to kill list of %s ids. First one is %s" % (len(killlist), killlist[0] ))
                killids(killlist)
            else:
                self.log.debug("No VM jobs to kill for apfqueue %s" % self.apfqueue.apfqname )
        except NotImplementedError:
            self.log.debug("Apparently using batchstatus plugin without job info. Skipping.")
        
            
