#!/bin/env python

from vc3remotemanager.ssh import SSHmanager
from vc3remotemanager.cluster import Cluster
from vc3remotemanager.bosco import Bosco

class Manage(object):
    """
    Set up remote target
    """

    def _checktarget(self, user, host, port, batch, pubkeyfile, privkeyfile, passfile=None):
        """
        Ensure remote_manager has set up rgahp
        """
        #Ensure paths
        pubkeyfile = os.path.expanduser(pubkeyfile)
        privkeyfile = os.path.expanduser(privkeyfile)
        try:
            passfile = os.path.expanduser(passfile)
        except AttributeError:
            pass

        # set up paramiko and stuff
        ssh = SSHManager(host, port, user, privkeyfile)
        cluster = Cluster(ssh)
        bosco = Bosco(cluster, ssh, batch, "1.2.10", "ftp://ftp.cs.wisc.edu/condor/bosco", None, "/tmp/bosco", "~/.condor", None, None, None, None)
        
        self.log.debug("Checking to see if remote gahp is installed and up to date...")
        try:
            clusters = bosco.get_clusters()
            entry = user + "@" + host
            if entry in clusters:
                self.log.debug("Cluster %s is already setup:" % entry)
            else:
                bosco.setup_bosco()
        except Exception, e:
            self.log.exception("Exception during bosco remote installation. ")
