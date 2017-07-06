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

from setuptools import setup

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

etc_files = ['etc/autopyfactory.conf',
             'etc/queues.conf',
             'etc/proxy.conf',
             'etc/monitor.conf',
             'etc/mappings.conf',
             'etc/agisdefaults.conf',
             'etc/auth.conf',
             ]

sysconfig_files = [
             'etc/sysconfig/autopyfactory',
             'etc/sysconfig/proxymanager',
]

logrotate_files = ['etc/logrotate/autopyfactory',]

initd_files = ['etc/autopyfactory',
               'etc/proxymanager']

# NOTES: the docs are actually handled by setup.cfg. They are moved directory under /usr/share/doc/autopyfactory-<version>/
docs_files = ['docs/%s' %file for file in os.listdir('docs') if os.path.isfile('docs/%s' %file)]


# -----------------------------------------------------------

rpm_data_files=[#('/etc/autopyfactory', libexec_files),
                ('/etc/autopyfactory', etc_files),
                ('/etc/sysconfig', sysconfig_files),
                ('/etc/logrotate.d', logrotate_files),                                        
                ('/etc/init.d', initd_files),
                #('/usr/share/doc/autopyfactory', docs_files),                                        
               ]


home_data_files=[#('etc', libexec_files),
                 ('etc/autopyfactory', etc_files),
                 ('etc/autopyfactory', initd_files),
                 ('etc/autopyfactory', sysconfig_files),
                 ('doc/autopyfactory', docs_files),
                ]





# setup for distutils
setup(
    name="autopyfactory",
    version=release_version,
    description='autopyfactory package',
    long_description="""This package contains autopyfactory""",
    license='GPL',
    author='Panda Team',
    author_email='hn-atlas-panda-pathena@cern.ch',
    maintainer='Jose Caballero',
    maintainer_email='jcaballero@bnl.gov',
    url='https://twiki.cern.ch/twiki/bin/view/Atlas/PanDA',
    packages=['autopyfactory',
              'autopyfactory.plugins',
              'autopyfactory.plugins.authmanager',
              'autopyfactory.plugins.authmanager.auth',
              'autopyfactory.plugins.factory',
              'autopyfactory.plugins.factory.config',
              'autopyfactory.plugins.queue',
              'autopyfactory.plugins.queue.batchstatus',
              'autopyfactory.plugins.queue.batchsubmit',
              'autopyfactory.plugins.queue.monitor',
              'autopyfactory.plugins.queue.sched',
              'autopyfactory.plugins.queue.wmsstatus',
              'autopyfactory.external',
              'autopyfactory.external.panda',
              ],
    scripts = [ # Utilities and main script
               'bin/autopyfactory',
               'bin/autopyfactory_version',
               'bin/proxymanager'
              ],
    
    data_files = rpm_data_files
    )
