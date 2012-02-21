#!/usr/bin/env python
#
# Setup prog for autopyfactory
#
#
release_version='2.1.0'

import re
import sys
import commands
from distutils.core import setup
from distutils.command.install import install as install_org
from distutils.command.install_data import install_data as install_data_org

# Python version check. 
major, minor, release, st, num = sys.version_info
if major == 2:
    if not minor >= 4:
        print("Autopyfactory requires Python >= 2.4. Exitting.")
        sys.exit(0)

rpm_data_files=[  ('/usr/libexec', ['libexec/runpilot3-wrapper.sh',
                                  'libexec/wrapper.sh',                                 
                                  ]),
                  ('/etc/apf', ['etc/factory.conf-example',
                              'etc/queues.conf-example',
                              'etc/proxy.conf-example',
                              'etc/factory.sysconfig-example'
                             ]),
                  ('/etc/init.d', ['etc/factory',
                                ]),
                  ('/etc/logrotate.d', ['etc/factory.logrotate',
                                ]),                                        
                ]

home_data_files=[('libexec', ['libexec/runpilot3-wrapper.sh',
                                  'libexec/wrapper.sh',                                 
                                 ]),
                ('etc', [ 'etc/factory.conf-example',
                          'etc/queues.conf-example',
                          'etc/proxy.conf-example',
                          'etc/factory.sysconfig-example'
                             ]),
                ('etc', ['etc/factory',
                                ]),      
                ]

def choose_data_files():
    #print(sys.argv)
    if 'bdist_rpm' in sys.argv:
        return rpm_data_files
    elif '--home' in sys.argv and 'install' in sys.argv:
        return home_data_files
    else:
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
               'misc/apfqueue_status.sh'],
    
    data_files = choose_data_files()
)
