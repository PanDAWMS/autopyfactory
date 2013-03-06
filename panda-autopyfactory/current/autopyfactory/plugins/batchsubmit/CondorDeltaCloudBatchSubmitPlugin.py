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
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorDeltaCloudBatchSubmitPlugin(CondorGridBatchSubmitPlugin):
    id = 'condordeltacloud'
    
    def __init__(self, apfqueue):

        super(CondorDeltaCloudBatchSubmitPlugin, self).__init__(apfqueue)
        self.log.info('CondorDeltaCloudBatchSubmitPlugin: Object initialized.')

    def _readconfig(self, qcl=None):
        '''
        read the config file
        '''

        # Chosing the queue config object, depending on 
        if not qcl:
            qcl = self.apfqueue.factory.qcl

        # we rename the queue config variables to pass a new config object to parent class
        newqcl = qcl.clone().filterkeys('batchsubmit.condordeltacloud', 'batchsubmit.condorgrid')
        valid = super(CondorDeltaCloudBatchSubmitPlugin, self)._readconfig(newqcl)
        if not valid:
            return False
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.gridresource', logger=self.log) 
            self.username = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.username', logger=self.log) 
            self.password_file = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.password_file', logger=self.log) 
            self.image_id = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.image_id', logger=self.log) 
            self.keyname = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.keyname', logger=self.log) 
            self.realm_id = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.realm_id', logger=self.log) 
            self.hardware_profile = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.hardware_profile', logger=self.log) 
            self.hardware_profile_memory = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.hardware_profile_memory', logger=self.log) 
            self.hardware_profile_cpu = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.hardware_profile_cpu', logger=self.log) 
            self.hardware_profile_storage = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.hardware_profile_storage', logger=self.log) 
            self.user_data = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.user_data', logger=self.log) 

            return True
        except:
            return False


    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorDeltaCloudBatchSubmitPlugin.addJSD: Starting.')

        self.JSD.add('grid_resource=deltacloud %s' % self.gridresource) 

        self.JSD.add("deltacloud_username=%s" % self.username) 
        self.JSD.add("deltacloud_password_file=%s" % self.password_file) 

        if self.image_id:
            self.JSD.add('deltacloud_image_id=%s' % self.image_id)          
        if self.keyname:
            self.JSD.add('deltacloud_keyname=%s' % self.keyname)          
        if self.realm_id:
            self.JSD.add('delta_realm_id=%s' %self.realm_id)
        if self.hardware_profile:
            self.JSD.add('delta_hardware_profile=%s' %self.hardware_profile)
        if self.hardware_profile_memory:
            self.JSD.add('delta_hardware_profile_memory=%s' %self.hardware_profile_memory)
        if self.hardware_profile_cpu:
            self.JSD.add('delta_hardware_profile_cpu=%s' %self.hardware_profile_cpu)
        if self.hardware_profile_storage:
            self.JSD.add('delta_hardware_profile_storage=%s' %self.hardware_profile_storage)
        if self.user_data:
            self.JSD.add('delta_user_data=%s' %self.user_data)

        super(CondorDeltaCloudBatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorDeltaCloudBatchSubmitPlugin.addJSD: Leaving.')


