#!/usr/bin/env python
#
# Setup prog for autopyfactory
#
#

from autopyfactory import factory

release_version=factory.__version__

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

etc_files = ['etc/autopyfactory.conf-example',
             'etc/queues.conf-example',
             'etc/proxy.conf-example',
             'etc/monitor.conf-example',
             'etc/mappings.conf-example',
             ]

sysconfig_files = [
             'etc/sysconfig/autopyfactory-example',
             'etc/sysconfig/proxymanager-example',
]

logrotate_files = ['etc/logrotate/autopyfactory-example',]

initd_files = ['etc/autopyfactory',
               'etc/proxymanager']

# NOTES: the docs are actually handled by setup.cfg. They are moved directory under /usr/share/doc/autopyfactory-<version>/
docs_files = ['docs/%s' %file for file in os.listdir('docs') if os.path.isfile('docs/%s' %file)]

# at least for the time being, we are not going to distribute manpages, given disutils does not support them properly
#man_files = ['docs/man/%s' %file for file in os.listdir('docs/man') if os.path.isfile('docs/man/%s' %file)]
#man1_files = ['docs/man/%s' %file for file in os.listdir('docs/man') if os.path.isfile('docs/man/%s' %file) and file.endswith('.1')]
#man5_files = ['docs/man/%s' %file for file in os.listdir('docs/man') if os.path.isfile('docs/man/%s' %file) and file.endswith('.5')]


# -----------------------------------------------------------

rpm_data_files=[#('/etc/autopyfactory', libexec_files),
                #('/etc/autopyfactory', etc_files),
                #('/etc/sysconfig', sysconfig_files),
                #('/etc/logrotate.d', logrotate_files),                                        
                ('/etc/init.d', initd_files),
                #('/usr/share/doc/autopyfactory', docs_files),                                        
                #('/tmp', man_files),                                        
                #('/usr/share/man/man1', man1_files),
                #('/usr/share/man/man5', man5_files),
               ]


home_data_files=[#('etc', libexec_files),
                 ('etc', etc_files),
                 ('etc', initd_files),
                 ('etc', sysconfig_files),
                 ('doc/autopyfactory', docs_files),
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
    name="autopyfactory",
    version=release_version,
    description='autopyfactory package',
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
               'bin/autopyfactory',
               'bin/proxymanager'
              ],
    
    data_files = choose_data_files()
)
