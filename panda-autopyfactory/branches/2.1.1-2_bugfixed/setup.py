#!/usr/bin/env python
#
# Setup prog for autopyfactory
#
#
release_version='2.1.1'

import commands
import os
import re
import sys

from distutils.core import setup
from distutils.command.install import install as install_org
from distutils.command.install_data import install_data as install_data_org

# Python version check. 
major, minor, release, st, num = sys.version_info
if major == 2:
    if not minor >= 4:
        print("Autopyfactory requires Python >= 2.4. Exitting.")
        sys.exit(0)

# ===========================================================
#           data files
# ===========================================================

libexec_files = ['libexec/runpilot3-wrapper.sh',
                 'libexec/wrapper.sh',]

etc_files = ['etc/factory.conf-example',
             'etc/queues.conf-example',
             'etc/proxy.conf-example',
             'etc/factory.sysconfig-example',]

initd_files = ['etc/factory',]

logrotate_files = ['etc/factory.logrotate',]

docs_files = ['docs/%s' %file for file in os.listdir('docs') if os.path.isfile('docs/%s' %file)]

# -----------------------------------------------------------

rpm_data_files=[('/usr/libexec',       libexec_files),
                ('/etc/apf',           etc_files),
                ('/etc/init.d',        initd_files),
                ('/etc/logrotate.d',   logrotate_files),                                        
                ('/usr/share/doc/apf', docs_files),                                        
               ]

home_data_files=[('libexec', libexec_files),
                 ('etc',     etc_files),
                 ('etc',     initd_files),
                 ('doc/apf', docs_files ),
                ]

# ===========================================================

def choose_data_files():
    #print(sys.argv)
    rpminstall = True
    userinstall = False
     
    if 'bdist_rpm' in sys.argv:
        rpminstall = True

    elif 'install' in sys.argv:
        for a in sys.argv:
            if a.lower().startswith('--home'):
                rpminstall = False
                userinstall = True
                
    if rpminstall:
        return rpm_data_files
    elif userinstall:
        return home_data_files
    else:
        # Something probably went wrong, so punt
        return rpm_data_files
       
# setup for distutils
setup(
    name="panda-autopyfactory",
    version=release_version,
    description='panda-autopyfactory package',
    long_description='''This package contains autopyfactory''',
    license='GPL',
    author='Panda Team',
    author_email='hn-atlas-panda-pathena@cern.ch',
    maintainer='Jose Caballero',
    maintainer_email='jcaballero@bnl.gov',
    url='https://twiki.cern.ch/twiki/bin/view/Atlas/PanDA',
    packages=['autopyfactory','autopyfactory.plugins'],
    scripts = [ # Utilities and main script
               'bin/factory',
               'misc/apfqueue-status',
               'misc/apfqueue-jobs-by-status.sh'
              ],
    
    data_files = choose_data_files()
)
