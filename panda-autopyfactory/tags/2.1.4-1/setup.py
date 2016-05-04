#!/usr/bin/env python
#
# Setup prog for autopyfactory
#
#
release_version='2.1.4'

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
#                D A T A     F I L E S 
# ===========================================================

libexec_files = ['libexec/%s' %file for file in os.listdir('libexec') if os.path.isfile('libexec/%s' %file)]

etc_files = ['etc/factory.conf-example',
             'etc/queues.conf-example',
             'etc/proxy.conf-example',
             'etc/monitor.conf-example',
             'etc/factory.sysconfig-example',]

initd_files = ['etc/factory',]

logrotate_files = ['etc/factory.logrotate',]

# docs files:
#   --everything in the docs/ directory
#   -- RELEASE_NOTES file 
docs_files = ['docs/%s' %file for file in os.listdir('docs') if os.path.isfile('docs/%s' %file)]
docs_files.append('RELEASE_NOTES')

utils_files = ['misc/apf-agis-config',
               'misc/apf-queue-status',
               'misc/apf-queue-jobs-by-status.sh',
               'misc/apf-test-pandaclient',
               ]

# -----------------------------------------------------------

rpm_data_files=[('/etc/apf',           libexec_files),
                ('/etc/apf',           etc_files),
                ('/etc/init.d',        initd_files),
                ('/etc/logrotate.d',   logrotate_files),                                        
                ('/usr/share/doc/apf', docs_files),                                        
                ('/usr/share/apf',     utils_files),                                        
               ]

home_data_files=[('etc',       libexec_files),
                 ('etc',       etc_files),
                 ('etc',       initd_files),
                 ('doc/apf',   docs_files ),
                 ('share/apf', utils_files),                                        
                ]

# -----------------------------------------------------------

def choose_data_files():
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
       
# ===========================================================

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
    packages=['autopyfactory',
              'autopyfactory.plugins',
              'autopyfactory.plugins.batchstatus',
              'autopyfactory.plugins.batchsubmit',
              'autopyfactory.plugins.monitor',
              'autopyfactory.plugins.sched',
              'autopyfactory.plugins.wmsstatus',
              ],
    scripts = [ # Utilities and main script
               'bin/factory',
              ],
    
    data_files = choose_data_files()
)
