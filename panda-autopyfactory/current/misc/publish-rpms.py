#!/bin/env python
# 
# Create RPMs, move to dist area, regenerate repos. 
# Should be run from the svn checkout root. 
# User must have GPG keys and .rpmmacros for signing. 
# Platform must be specified. 


import subprocess 
import sys
import logging
import os
import re
import shutil


PKGNAME="panda-autopyfactory"
REPOROOT="/afs/usatlas.bnl.gov/mgmt/repo/grid"
REGENCMD="/afs/usatlas.bnl.gov/mgmt/repo/regen-repos.py"
ARCHS=['i386','x86_64']
PLATFORMS=['fedora','rhel']
REPOS=['development','testing','production']
USAGE="apf-deploy.py <repos>"
RELEASEMAP={ 'Fedora release 14 (Laughlin)' : ('fedora','14'),
             'Fedora release 16 (Verne)' : ('fedora','16'),
             'Red Hat Enterprise Linux Client release 5.7 (Tikanga)' : ('rhel','5Client'),
             'Red Hat Enterprise Linux Client release 5.8 (Tikanga)' : ('rhel','5Client'),
             'Red Hat Enterprise Linux Client release 5.9 (Tikanga)' : ('rhel','5Client'),
             'Red Hat Enterprise Linux Workstation release 6.2 (Santiago)' : ('rhel','6Workstation'),
             'Red Hat Enterprise Linux Workstation release 6.3 (Santiago)' : ('rhel','6Workstation'),            
             'Red Hat Enterprise Linux Workstation release 6.4 (Santiago)' : ('rhel','6Workstation'),            
             'Red Hat Enterprise Linux Workstation release 6.5 (Santiago)' : ('rhel','6Workstation'),            

            }
RPMGLOB='.*.noarch.rpm$'
RPMRE=re.compile(RPMGLOB, re.IGNORECASE)


class DeployManager(object):
    def __init__(self, repos):
        logging.basicConfig(level=logging.DEBUG)
        logging.debug('DeployManager initialized.')
        self.repos = repos
    
    def getplatform(self):
        f = open('/etc/redhat-release','r')
        rs = f.readline()
        rs = rs.strip()
        logging.debug("release string: %s" % rs)
        (self.dist, self.distver) = RELEASEMAP[rs] 
        logging.info("Distribution=%s, Version=%s" % (self.dist, self.distver))
    
    def build(self):
        cmd = 'python setup.py bdist_rpm'
        logging.info("Running build command: '%s'" % cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)     
        (out, err) = p.communicate()
        print(out)
        print(err)
        if p.returncode == 0:
            logging.info('Build OK')
        else:
            raise 
    
    def copy(self):
        '''
        Copy files to repo area...
        '''
        r = RPMRE
        allfiles = os.listdir('dist')
        logging.debug("allfiles = %s" % allfiles)
        
        # find .noarch.rpm files.
        matches = []
        for f in allfiles:
            m = r.match(f)
            if m:
                logging.debug("%s matches." % f)
                matches.append(f)
            else:
                logging.debug("%s doesn't match." % f)
        
        # copy them to appropriate places. 
        for f in matches:
            for r in self.repos:
                for a in ARCHS:
                    src = 'dist/%s' % f
                    dst = "%s/%s/%s/%s/%s/" % ( REPOROOT, 
                                          r ,
                                          self.dist,
                                          self.distver,
                                          a ) 
                    logging.info("%s -> %s" % (src,dst))
                    shutil.copy(src,dst)
    
    def regen(self):
        '''
        Regenerate repository metadata...
        '''
        cmd = REGENCMD
        logging.info("Running regen command: '%s'" % cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)     
        (out, err) = p.communicate()
        print(out)
        if p.returncode == 0:
            logging.info('Repos regenerated OK')
        else:
            raise 

def main():
    print(sys.argv)
    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit(0)
    else:
        targets = sys.argv[1:]
        repos = []
        for t in targets:
            if t not in REPOS:
                print(USAGE)
                logging.error("ERROR: Valid repos are (%s)." % ' '.join(REPOS))
                sys.exit(0)
            else:
                repos.append(t)
        
        #print("targets: %s" % targets)
        try:
            dm = DeployManager(repos)
            dm.getplatform()
            dm.build()
            dm.copy()
            dm.regen()
        except Exception, e:
            logging.error("ERROR: %s" % str(e) )
    
if __name__=='__main__':
    main()
    
    
    




