#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin
import jsd 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorEC2BatchSubmitPlugin(CondorGridBatchSubmitPlugin):
        id = 'condorec2'
        
        def __init__(self, apfqueue):

                super(CondorEC2BatchSubmitPlugin, self).__init__(apfqueue)
                self.log.info('CondorEC2BatchSubmitPlugin: Object initialized.')

        def _readconfig(self, qcl=None):
                '''
                read the config file
                '''

                # Chosing the queue config object, depending on 
                if not qcl:
                    qcl = self.apfqueue.factory.qcl

                # we rename the queue config variables to pass a new config object to parent class
                newqcl = qcl.clone().filterkeys('batchsubmit.condorec2', 'batchsubmit.condorgrid')
                valid = super(CondorEC2BatchSubmitPlugin, self)._readconfig(newqcl)
                if not valid:
                        return False
                try:
                        self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.gridresource', logger=self.log) 
                        self.ami_id = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.ami_id', logger=self.log)
                        self.instance_type  = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.instance_type', logger=self.log)
                        self.user_data = qcl.generic_get(self.apfqname, 'batchsubmit.condorec2.user_data', logger=self.log)
                        self.access_key_id = qcl.generic_get(self.apfqname,'batchsubmit.condorec2.access_key_id', logger=self.log)
                        self.secret_access_key = qcl.generic_get(self.apfqname,'batchsubmit.condorec2.secret_access_key', logger=self.log)
                        return True
                except:
                        return False


        def _addJSD(self):
                '''
                add things to the JSD object
                '''

                self.log.debug('CondorEC2BatchSubmitPlugin.addJSD: Starting.')

                super(CondorEC2BatchSubmitPlugin, self)._addJSD()

                self.JSD.add('grid_resource=ec2 %s' % self.gridresource) 

                # -- proxy path --
                #self.JSD.add("ec2_access_key_id=%s" % self.x509userproxy) 
                #self.JSD.add("ec2_secret_access_key=%s" % self.x509userproxy) 
                self.JSD.add("ec2_access_key_id=%s" % self.access_key_id) 
                self.JSD.add("ec2_secret_access_key=%s" % self.secret_access_key) 

                # -- EC2 specific parameters --
                self.JSD.add("ec2_ami_id=%s" % self.ami_id) 
                self.JSD.add("ec2_instance_type=%s" % self.instance_type) 
                if self.user_data:
                    self.JSD.add('ec2_user_data=%s' % self.user_data)              

                self.log.debug('CondorEC2BatchSubmitPlugin.addJSD: Leaving.')


