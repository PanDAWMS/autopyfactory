#!/usr/bin/env python
#
# Setup prog for autopyfactory
#
#
release_version='1.0.2'

import re
import sys
import commands
from distutils.core import setup
from distutils.command.install import install as install_org
from distutils.command.install_data import install_data as install_data_org

        
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
    data_files=[
                # Utilitys and main script
                ('/opt/panda/autopyfactory/bin', ['bin/factory.py',
                                                  'bin/cleanLogs.py',
                                                  'bin/vomsrenew.sh',
                                                  ]
                 ),
                ('/opt/panda/autopyfactory/libexec', ['libexec/runpilot3-wrapper.sh',
                                                      ]
                 ),
                ('/opt/panda/autopyfactory/etc', ['etc/factory.conf-example',
                                                  ]),
                ]
)
