#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin
from autopyfactory import jsd 
from autopyfactory.condor import killids, mincondorversion
import subprocess
import time
import commands
import logging
import traceback

mincondorversion(8,1,1)


class CondorEC2BatchSubmitPlugin(CondorGridBatchSubmitPlugin):
    id = 'condorec2'
    
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.factory.qcl            
        else:
            qcl = config
        
        newqcl = qcl.clone().filterkeys('batchsubmit.condorec2', 'batchsubmit.condorgrid')
        super(CondorEC2BatchSubmitPlugin, self).__init__(apfqueue, newqcl)
       
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.gridresource') 
            self.ami_id = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.ami_id')
            self.instance_type  = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.instance_type')
            self.user_data = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.user_data')
            self.access_key_id = qcl.generic_get(self.apfqname,'batchsubmit.condorec2.access_key_id')
            self.secret_access_key = qcl.generic_get(self.apfqname,'batchsubmit.condorec2.secret_access_key')
            self.spot_price = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.spot_price')
            self.usessh = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.usessh')
            if self.usessh == "False":
                self.usessh = False
            elif self.usessh == "True":
                self.usessh = True
            else:
                self.usessh = False
            if self.spot_price:                
                self.spot_price = float(self.spot_price)
            self.security_groups = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.security_groups')
            self.log.debug("Successfully got all config values for EC2BatchSubmit plugin.")
            self.log.info('CondorEC2BatchSubmitPlugin: Object properly initialized.')
        except Exception, e:
            self.log.error("Problem getting object configuration variables.")
            self.log.debug("Exception: %s" % traceback.format_exc())

    def submit(self, num):
        '''
        Override base submit to determine if we need to *unretire* any nodes. 
        
        retiring_pilots = self.batchinfo[self.apfqueue.apfqname].retiring
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)
        
        '''
        if num < 1:
            self.log.debug("Number to submit is zero or negative, calling parent...")
            super(CondorEC2BatchSubmitPlugin, self).submit(num)
        else:
            self.log.debug("Checking for jobs in 'retiring' state...")
            batchinfo = self.apfqueue.batchstatus_plugin.getInfo(queue = self.apfqueue.apfqname, maxtime = self.apfqueue.batchstatusmaxtime)
            numretiring = batchinfo.retiring
            self.log.debug("%d jobs in 'retiring' state." % numretiring)
            numleft = num - numretiring
            if numleft > 0:
                self.log.debug("More to submit (%d) than retiring (%d). Unretiring all and submitting %d" % (num, 
                                                                                                             numretiring,
                                                                                                             numleft) )
                self.unretire(numretiring)
                super(CondorEC2BatchSubmitPlugin, self).submit(numleft)
            else:
                self.log.debug("Fewer to submit than retiring. Unretiring %d" % num)
                self.unretire(num)
               
    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorEC2BatchSubmitPlugin.addJSD: Starting.')
        super(CondorEC2BatchSubmitPlugin, self)._addJSD()

        self.JSD.add('grid_resource', 'ec2 %s' % self.gridresource) 

        # -- proxy path --
        self.JSD.add("ec2_access_key_id", "%s" % self.access_key_id) 
        self.JSD.add("ec2_secret_access_key", "%s" % self.secret_access_key) 

        # -- EC2 specific parameters --
        self.JSD.add("ec2_ami_id", "%s" % self.ami_id) 
        self.JSD.add("executable", "%s" % self.apfqueue.apfqname)
        self.JSD.add("ec2_instance_type", "%s" % self.instance_type) 
        if self.user_data:
            self.JSD.add('ec2_user_data', '%s' % self.user_data)          
        if self.spot_price:
            self.JSD.add('ec2_spot_price', '%f' % self.spot_price)
        if self.security_groups:
            self.JSD.add('ec2_security_groups', '%s' % self.security_groups)

        self.log.debug('CondorEC2BatchSubmitPlugin.addJSD: Leaving.')

       
    def unretire(self, n ):
        '''
        trigger unretirement of n nodes. 
        
        '''
        if n > 0:
            self.log.info("Beginning to unretire %d VM jobs..." % n)
            jobinfo = self.apfqueue.batchstatus_plugin.getJobInfo(queue=self.apfqueue.apfqname)
            if jobinfo:
                numtounretire = n
                numunretired = 0
                for job in jobinfo:
                    self.log.debug("Handling instanceid =  %s" % job.executeinfo.instanceid)
                    stat = job.executeinfo.getStatus()
                    if stat  == 'retiring':
                        self._unretirenode(job)
                        numtounretire = numtounretire - 1
                        numunretired += 1
                        self.log.debug("numtounretire = %d" % numtounretire)
                        if numtounretire <= 0:
                            break
                self.log.info("Retired %d VM jobs" % numunretired)
            else:
                self.log.info("Some info unavailable. Do nothing.")    
        else:
            self.log.debug("No jobs to unretire...")

    def retire(self, n, order='oldest'):
        '''
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
        '''
        self.log.info("Beginning to retire %d VM jobs..." % n)
        jobinfo = self.apfqueue.batchstatus_plugin.getJobInfo(queue=self.apfqueue.apfqname)
        if jobinfo:
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
            self.log.info("Retired %d VM jobs" % numretired)
        else:
            self.log.info("Some info unavailable. Do nothing.")


    
    def _retirenode(self, jobinfo):
        '''
        Do whatever is needed to tell the node to retire...
        '''
        self.log.info("Retiring node %s (%s)" % (jobinfo.executeinfo.hostname, 
                                                 jobinfo.ec2instancename))
        exeinfo = jobinfo.executeinfo
        publicip = exeinfo.hostname
        machine = exeinfo.machine
        condorid = "%s.%s" % (jobinfo.clusterid, jobinfo.procid)
        
        if self.usessh:
            self.log.info("Trying to use SSH to retire node %s" % publicip)
            cmd='ssh root@%s "condor_off -peaceful -startd"' % publicip
            self.log.debug("retire cmd is %s" % cmd) 
            before = time.time()
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            out = None
            (out, err) = p.communicate()
            delta = time.time() - before
            self.log.debug('It took %s seconds to issue the command' %delta)
            self.log.info('%s seconds to issue command' %delta)
            if p.returncode == 0:
                self.log.debug('Leaving with OK return code.')
            else:
                self.log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))          
            # invoke ssh to retire node
        else:
            # call condor_off locally
            self.log.info("Trying local retirement of node %s" % publicip)
            if machine.strip() != "":
                cmd='condor_off -peaceful -startd -name %s' % machine
                self.log.debug("retire cmd is %s" % cmd) 
                before = time.time()
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                out = None
                (out, err) = p.communicate()
                delta = time.time() - before
                self.log.debug('It took %s seconds to issue the command' %delta)
                self.log.info('%s seconds to issue command' %delta)
                if p.returncode == 0:
                    self.log.debug('Leaving with OK return code.')
                else:
                    out = out.replace("\n", " ")
                    out = err.replace("\n", " ")
                    self.log.warning('Leaving with bad return code. rc=%s err="%s" out="%s"' %(p.returncode, err, out ))          
            else:
                self.log.warning("Unable to retire node %s (%s) because it has an empty machine name." % (jobinfo.executeinfo.hostname,
                                                                                                          jobinfo.ec2instancename))
                

    def _unretirenode(self, jobinfo):
        '''
        Do whatever is needed to tell the node to un-retire...
        '''
        self.log.info("Unretiring node %s (%s)" % (jobinfo.executeinfo.hostname, 
                                                 jobinfo.ec2instancename))
        exeinfo = jobinfo.executeinfo
        publicip = exeinfo.hostname
        machine = exeinfo.machine
        condorid = "%s.%s" % (jobinfo.clusterid, jobinfo.procid)
        
        if self.usessh:
            self.log.info("Trying to use SSH to retire node %s" % publicip)
            cmd='ssh root@%s "condor_on -startd"' % publicip
            self.log.debug("unretire cmd is %s" % cmd) 
            before = time.time()
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            out = None
            (out, err) = p.communicate()
            delta = time.time() - before
            self.log.debug('It took %s seconds to issue the command' %delta)
            self.log.info('%s seconds to issue command' %delta)
            if p.returncode == 0:
                self.log.debug('Leaving with OK return code.')
            else:
                self.log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))          
            # invoke ssh to retire node
        else:
            if machine.strip() != "":
                # call condor_off locally
                self.log.info("Trying local unretirement of node %s" % publicip)
            else:
                self.log.warning("Unable to unretire node %s (%s) because it has an empty machine name." % (jobinfo.executeinfo.hostname,
                                                                                                          jobinfo.ec2instancename))
             

    def cleanup(self):
        '''
        
        '''
        self.log.debug("Cleanup called in EC2. Retiring...")
        self._killretired()

        
    def _killretired(self):
        '''
        scan through jobinfo for this queue with job
        
        '''
        self.log.info("Killretired process triggered. Searching...")
        jobinfo = self.apfqueue.batchstatus_plugin.getJobInfo(queue=self.apfqueue.apfqname)
        self.log.info("Finding and killing VM jobs in 'retired' state.")
        
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
            self.log.info("About to kill list of %s ids. First one is %s" % (len(killlist), killlist[0] ))
            killids(killlist)
        else:
            self.log.info("No VM jobs to kill for apfqueue %s" % self.apfqueue.apfqname )

        
            
