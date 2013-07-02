#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin
from autopyfactory import jsd 
from autopyfactory.condor import killids
import subprocess
import time


class CondorEC2BatchSubmitPlugin(CondorGridBatchSubmitPlugin):
    id = 'condorec2'
    
    def __init__(self, apfqueue):

        super(CondorEC2BatchSubmitPlugin, self).__init__(apfqueue)
        self.log.info('CondorEC2BatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl=None):
        '''
        read the config file
        do housekeeping!
        
        XXX self._killretired doesn't belong here, but it was the only place to
        unconditionally get called in the plugin every cycle. 
        
        '''

        # Choosing the queue config object, depending on 
        if not qcl:
            qcl = self.apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condorec2', 'batchsubmit.condorgrid')
        valid = super(CondorEC2BatchSubmitPlugin, self)._readconfig(newqcl)
        if not valid:
            return False
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.gridresource') 
            self.ami_id = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.ami_id')
            self.instance_type  = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.instance_type')
            self.user_data = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.user_data')
            self.access_key_id = qcl.generic_get(self.apfqname,'batchsubmit.condorec2.access_key_id')
            self.secret_access_key = qcl.generic_get(self.apfqname,'batchsubmit.condorec2.secret_access_key')
            self.spot_price = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.spot_price')
            if self.spot_price:
                self.spot_price = float(self.spot_price)
            self.security_groups = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.security_groups')
            return True
        except:
            return False


    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorEC2BatchSubmitPlugin.addJSD: Starting.')
        super(CondorEC2BatchSubmitPlugin, self)._addJSD()

        self.JSD.add('grid_resource=ec2 %s' % self.gridresource) 

        # -- proxy path --
        self.JSD.add("ec2_access_key_id=%s" % self.access_key_id) 
        self.JSD.add("ec2_secret_access_key=%s" % self.secret_access_key) 

        # -- EC2 specific parameters --
        self.JSD.add("ec2_ami_id=%s" % self.ami_id) 
        self.JSD.add("executable=%s" % self.apfqueue.apfqname)
        self.JSD.add("ec2_instance_type=%s" % self.instance_type) 
        if self.user_data:
            self.JSD.add('ec2_user_data=%s' % self.user_data)          
        if self.spot_price:
            self.JSD.add('ec2_spot_price=%f' % self.spot_price)
        if self.security_groups:
            self.JSD.add('ec2_security_groups=%s' % self.security_groups)

        self.log.debug('CondorEC2BatchSubmitPlugin.addJSD: Leaving.')

    def xxxsubmit(self, n):
        '''
        
        1) unretire r retiring/retired nodes if r < n
        2) if n > r, submit n-r new jobs
        3) terminate nodes that are in 'retired' state. 
        '''
        statusinfo = self.apfqueue.batchstatus_plugin.getInfo()
        jobinfo = self.apfqueue.batchstatus_plugin.getJobInfo()
        
        if n>0:
            pass
            
        elif n < 0:
            self.retire(abs(n))
        
        self._killretired()
        
        

    def retire(self, n, order='oldest'):
        '''
        trigger retirement of this many nodes, but looking at this parent APF queue's 
        CondorCloudBatchStatus plugin. 
        
        Scan jobinfo for node start times
           execute condor_off -peaceful -daemon startd -name <machine>
        OR
           ssh <EC2PublicIP> condor_off -peaceful -daemon startd
                
        for each desired retirement. 
        
        '''
        self.log.info("Beginning to retire %d VM jobs..." % n)
        jobinfo = self.apfqueue.batchstatus_plugin.getJobInfo()
        if jobinfo:
            thisqueuejobs = jobinfo[self.apfqueue.apfqname]
            numtoretire = n
            numretired = 0
            for job in thisqueuejobs:
                self.log.debug("Handling instanceid =  %s" % job.executeinfo.instanceid)
                stat = job.executeinfo.getStatus()
                if stat  == 'busy' or stat == 'idle':
                    self._retirenode(job)
                    numtoretire = numtoretire - 1
                    numretired += 1
                    self.log.debug("numtoretire = %d" % numtoretire)
                    if numtoretire <= 0:
                        break
            self.log.info("Retired %d VM jobs" % numretired)
        else:
            self.log.info("Some info unavailable. Do nothing.")
    
    def _retirenode(self, jobinfo, usessh=True):
        '''
        Do whatever is needed to tell the node to retire...
        '''
        self.log.info("Retiring node %s (%s)" % (jobinfo.executeinfo.hostname, 
                                                 jobinfo.ec2instancename))
        exeinfo = jobinfo.executeinfo
        publicip = exeinfo.hostname
        machine = exeinfo.machine
        condorid = "%s.%s" % (jobinfo.clusterid, jobinfo.procid)
        
        if usessh:
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
        jobinfo = self.apfqueue.batchstatus_plugin.getJobInfo()
        self.log.info("Finding and killing VM jobs in 'retired' state.")
        
        killlist = []
        if jobinfo:
            myjobs = jobinfo[self.apfqueue.apfqname]        
            for j in myjobs:
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
            
