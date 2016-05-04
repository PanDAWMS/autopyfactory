#!/usr/bin/env python
#
# Setup prog for autopyfactory
#
#
release_version='1.2.0'

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

        
# setup for distutils
setup(
    name="panda-autopyfactory",
    version=release_version,
    description='panda-autopyfactory package',
    long_description='''This package contains autopyfactory''',
    license='GPL',
    author='Panda Team',
    author_email='hn-atlas-panda-pathena@cern.ch',
    maintainer='Graeme Stewart',
    maintainer_email='graeme.andrew.stewart@cern.ch',
    url='https://twiki.cern.ch/twiki/bin/view/Atlas/PanDA',
    packages=['autopyfactory'],
    scripts = [ # Utilities and main script
                'bin/factory.py'],
    
    data_files=[('libexec', ['libexec/runpilot3-wrapper.sh',
                              'libexec/wrapper.sh',                                 
                            ]),
                ('/etc/apf', ['etc/factory.conf-example',
                             ]),
                ('/etc/init.d', ['etc/factory',
                                ]),
                ('/etc/sysconfig', ['etc/factory.sysconfig',
                                   ]),                                         
                ]
)
