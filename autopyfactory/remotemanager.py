#!/bin/env python

import logging
import os

from vc3remotemanager.ssh import SSHManager
from vc3remotemanager.cluster import Cluster
from vc3remotemanager.bosco import Bosco

# Added to support running module as script from arbitrary location.
from os.path import dirname, realpath, sep, pardir
fullpathlist = realpath(__file__).split(sep)
prepath = sep.join(fullpathlist[:-2])
import sys
sys.path.insert(0, prepath)

class Manage(object):
    """
    Set up remote target
    """

    def __init__(self):
	self.log = logging.getLogger('autopyfactory')
	self.log.debug("Initializing remote manager module...")

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

        installdir = "~/.condor"

        # set up paramiko and stuff
        ssh = SSHManager(host, port, user, privkeyfile)
        cluster = Cluster(ssh)
	# fixme - move these into defaults
        bosco = Bosco(cluster, ssh, batch, "1.2.10", "ftp://ftp.cs.wisc.edu/condor/bosco", None, "/tmp/bosco", installdir, None, None, None, None)
        
        self.log.debug("Checking to see if remote gahp is installed and up to date...")
        try:
            clusters = bosco.get_clusters()
            entry = user + "@" + host
            if entry in clusters:
                self.log.debug("Cluster %s is already setup:" % entry)
            else:
                self.log.debug("Didn't find cluster %s, installing..." % entry)
                bosco.setup_bosco()
        except Exception, e:
            self.log.exception("Exception during bosco remote installation. ")

        # add a return for the location of the glite installation
        glite = installdir + "/bosco/glite"
        return glite

if __name__ == '__main__':
    # some simple tests
	host = "uct3-s1.mwt2.org"
	port = 22
	user = "lincolnb"
	privkeyfile = "/home/autopyfactory/etc/autopyfactory/auth/lincolnb.uchicago-uct3/ssh-rsa"
	pubkeyfile = "/home/autopyfactory/etc/autopyfactory/auth/lincolnb.uchicago-uct3/ssh-rsa.pub"
	batch = "condor"

	# Set up logging.
	debug = True
	info = 0

	# Check python version
	major, minor, release, st, num = sys.version_info

	# Set up logging, handle differences between Python versions...
	# In Python 2.3, logging.basicConfig takes no args
	#
	FORMAT23="[ %(levelname)s ] %(asctime)s %(filename)s (Line %(lineno)d): %(message)s"
	FORMAT24=FORMAT23
	FORMAT25="[%(levelname)s] %(asctime)s %(module)s.%(funcName)s(): %(message)s"
	FORMAT26=FORMAT25

	if major == 2:
		if minor ==3:
		    formatstr = FORMAT23
		elif minor == 4:
		    formatstr = FORMAT24
		elif minor == 5:
		    formatstr = FORMAT25
		elif minor == 6:
		    formatstr = FORMAT26
		elif minor == 7:
		    formatstr = FORMAT26

	log = logging.getLogger('autopyfactory')
	hdlr = logging.StreamHandler(sys.stdout)
	formatter = logging.Formatter(FORMAT23)
	hdlr.setFormatter(formatter)
	log.addHandler(hdlr)

	if debug:
		log.setLevel(logging.DEBUG) # Override with command line switches
	if info:
		log.setLevel(logging.INFO) # Override with command line switches
	log.debug("Logging initialized.")


	b = Manage()
	b._checktarget(user, host, port, batch, pubkeyfile, privkeyfile, None)
