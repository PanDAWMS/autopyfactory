#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#
#
# At init
# - confirm bosco local exists
# - get paths for auth tokens

#At submit
# -check if cluster is added to bosco
# -if not,
#    get lock
#    cp files
#    add cluster
#    test bosco
# - do submit

#  1) create ~/.ssh/bosco_key.rsa ~/.ssh/bosco_key.rsa.pub
#  2) create ~/.bosco/.pass   (passphrase for ssh key) 
#  bosco_cluster -a griddev03.racf.bnl.gov     (adds as PBS)
#  3) change ~/.bosco/clusterlist    <host>   entry=griddev03.racf.bnl.gov max_queued=-1 cluster_type=pbs  -> slurm
#  4) bosco_cluster --test griddev03.racf.bnl.gov

from autopyfactory import jsd
from autopyfactory import bosco

class CondorSSH(CondorBase):
    id = 'condorssh'
    lock = threading.Lock()
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
       
    def __init__(self, apfqueue, config=None):
        
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorssh', 'batchsubmit.condorbase')
        super(CondorBosco, self).__init__(apfqueue, config=newqcl) 
        # check local bosco install, will throw exeption if not present
        self._checkbosco()
        
        try:
            
            self.batch = qcl.generic_get(self.apfqname, 'batchsubmit.condorssh.batch')
            self.host = qcl.generic_get(self.apfqname, 'batchsubmit.condorssh.host')
            self.port = qcl.generic_get(self.apfqname,'batchsubmit.condorssh.port' )
            self.user = qcl.generic_get(self.apfqname,'batchsubmit.condorssh.user' )
            self.authprofile  = qcl.generic_get(self.apfqname,'batchsubmit.condorssh.authprofile' )
            self.pubkeyfile = None
            self.privkeyfile = None
            self.passfile = None
            self._getSSHAuthTokens()
            
            self._checkbosco()
            self._checktarget()
                        
            self.log.debug("SSH target attributes gathered from config. ")
        
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        
        self.log.info('CondorSSH: Object initialized.')

    def _getSSHAuthTokens(self):
        '''
        uses authmanager to find out the paths to SSH auth info
        '''
    
        self.log.trace("Determining proxy, if necessary. Profile: %s" % self.authprofile)
        (self.pubkeyfile, self.privkeyfile, self.passfile) = self.factory.authmanager.getSSHKeyPairPaths(self.authprofile)
        



    def submit(self, num):
        '''
        Override base submit. 
        
        '''
        self.log.debug("Entering bosco submit.")    
        joblist = super(CondorBosco, self).submit(num)
       
        self.log.debug("Exiting bosco submit.")






    def _addJSD(self):
        '''
        add things to the JSD object
        executable = probescript.sh
        arguments = -s 15
        Transfer_Executable = true
        universe = grid
        grid_resource = batch slurm griddev03.racf.bnl.gov --rgahp-key /home/me/privkey --rgahp-pass /home/me/mypassphrase
        #??? can host take port???
        output = output/$(Cluster).$(Process).out
        error= output/$(Cluster).$(Process).error
        log = output/$(Cluster).$(Process).log
        queue   
        
        grid_resource = batch pbs me@pbs.foo.edu --rgahp-key /home/me/privkey --rgahp-pass /home/me/mypassphrase      
        
        '''
        
        self.log.debug('CondorBosco.addJSD: Starting.')
        self.JSD.add("universe", "grid")
        self.JSD.add('grid_resource', 'batch %s %s:%s --rgahp-key %s --rgahp-pass %s' % (self.batch, 
                                                          self.host, 
                                                          self.port,
                                                          self.privkeyfile, 
                                                          self.passfile))
        self.JSD.add('+TransferOutput', '""')
        
        
        super(CondorBosco, self)._addJSD()
        self.log.debug('CondorBosco.addJSD: Leaving.')





