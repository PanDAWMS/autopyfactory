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
#
#   Two-hop SSH command:
#   ssh -A -t ssh01.sdcc.bnl.gov ssh -A -t icsubmit01.sdcc.bnl.gov 
#
#   Host midway-login1.rcc.uchicago.edu
#    User lincolnb
#    IdentityFile ~/.ssh/id_midway
#

import os
import shutil

from autopyfactory import jsd
from autopyfactory import remotemanager
from CondorBase import CondorBase

class CondorSSH(CondorBase):
    id = 'condorssh'
    """
    This class is expected to have separate instances for each PandaQueue object. 
    """
       
    def __init__(self, apfqueue, config, section):
        
        qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condorssh', 'batchsubmit.condorbase')
        super(CondorSSH, self).__init__(apfqueue, newqcl, section) 
        # check local bosco install, will throw exeption if not present
         
        try:
            
            self.batch = qcl.generic_get(self.apfqname, 'batchsubmit.condorssh.batch')
            self.host = qcl.generic_get(self.apfqname, 'batchsubmit.condorssh.host')
            self.port = qcl.generic_get(self.apfqname,'batchsubmit.condorssh.port' )
            self.user = qcl.generic_get(self.apfqname,'batchsubmit.condorssh.user' )
            self.authprofile  = qcl.generic_get(self.apfqname,'batchsubmit.condorssh.authprofile' )
            self.log.debug("SSH target attributes gathered from config. ")
            
            # Get auth info
            self.pubkeyfile = None
            self.privkeyfile = None
            self.passfile = None
            self._getSSHAuthTokens()
            
            # Back up user's SSH items
            self._backupSSHDefaults()
            
            # Unconditionally create ~/.ssh/config
            self._createSSHConfig()
            
            #Handle bosco
            self.log.debug("calling remote manager with options %s , %s , %s , %s , %s , %s , %s" % (self.user, self.host, self.port, self.batch, self.pubkeyfile, self.privkeyfile, self.passfile))
            self.rgahp = remotemanager.Manage()
            # rgahp._checktarget returns the glite installation dir
            self.glite = self.rgahp._checktarget(self.user,
                                       self.host, 
                                       self.port, 
                                       self.batch, 
                                       self.pubkeyfile, 
                                       self.privkeyfile, 
                                       self.passfile)
            
            self.log.info('CondorSSH: Object initialized.')
            
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        
    def _backupSSHDefaults(self):
        '''
        If they exist, copies to make a backup of: 
           ~/.ssh/id_rsa.pub
           ~/.ssh/id_rsa
           ~/.ssh/config
        '''
        SSHDEFAULTS = ['~/.ssh/id_rsa.pub',
                       '~/.ssh/id_rsa',
                       '~/.ssh/config'
                       ]
        for sshdefault in SSHDEFAULTS:
            dp = os.path.expanduser(sshdefault)
            if os.path.exists(dp):
                if not os.path.exists("%s.apfbackup" % dp):
                    self.log.debug("Backing up SSH file %s" % dp)
                    try:
                        shutil.copy(dp,"%s.apfbackup" % dp )
                    except Exception, e:
                        self.log.warning("Unable to back up %s" % dp)

    def _createSSHConfig(self):
        '''
        
        '''
        SSHCONFIG='''Host *
    StrictHostKeyChecking no
    UserKnownHostsFile=/dev/null
'''
        self.log.debug("Creating standard ssh config for APF submission.")
        f = os.path.expanduser('~/.ssh/config')
        fh = open(f, 'w')
        try:
            fh.write(SSHCONFIG)
        except Exception, e:
            self.log.error('Problem writing to ~/.ssh/config')
        finally:
            fh.close()
        os.chmod(f, 0600)

    def _getSSHAuthTokens(self):
        """
        uses authmanager to find out the paths to SSH auth info
        """    
        self.log.debug("Retrieving SSH auth token info. Profile: %s" % self.authprofile)
        (self.pubkeyfile, self.privkeyfile, self.passfile) = self.factory.authmanager.getSSHKeyPairPaths(self.authprofile)
        self.log.debug("Got paths: pubkey %s privkey %s passfile %s" % (self.pubkeyfile, 
                                                                        self.privkeyfile, 
                                                                        self.passfile))

    def _addJSD(self):
        """
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
        
        """
        
        self.log.debug('CondorBosco.addJSD: Starting.')
        self.JSD.add("universe", "grid")
        self.JSD.add('grid_resource', 'batch %s %s@%s --rgahp-key %s --rgahp-glite %s ' % (self.batch, 
                                                          self.user,
                                                          self.host, 
                                                          #self.port,
                                                          self.privkeyfile, 
                                                          #self.passfile,
                                                          self.glite
                                                          ) )
        self.JSD.add('+TransferOutput', '""')
        super(CondorSSH, self)._addJSD()
        self.log.debug('CondorBosco.addJSD: Leaving.')
