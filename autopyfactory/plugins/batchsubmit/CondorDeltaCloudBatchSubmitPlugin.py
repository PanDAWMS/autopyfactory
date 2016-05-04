#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin
import jsd 


class CondorDeltaCloudBatchSubmitPlugin(CondorGridBatchSubmitPlugin):
    id = 'condordeltacloud'
    
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
        newqcl = qcl.clone().filterkeys('batchsubmit.condordeltacloud', 'batchsubmit.condorgrid')
        super(CondorDeltaCloudBatchSubmitPlugin, self).__init__(apfqueue, config=newqcl)
        try:
            self.gridresource = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.gridresource') 
            self.username = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.username') 
            self.password_file = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.password_file') 
            self.image_id = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.image_id') 
            self.keyname = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.keyname') 
            self.realm_id = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.realm_id') 
            self.hardware_profile = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.hardware_profile') 
            self.hardware_profile_memory = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.hardware_profile_memory') 
            self.hardware_profile_cpu = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.hardware_profile_cpu') 
            self.hardware_profile_storage = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.hardware_profile_storage') 
            self.user_data = qcl.generic_get(self.apfqname, 'batchsubmit.condordeltacloud.user_data') 
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise
        
        self.log.info('CondorDeltaCloudBatchSubmitPlugin: Object initialized.')

    def _addJSD(self):
        '''
        add things to the JSD object
        '''

        self.log.debug('CondorDeltaCloudBatchSubmitPlugin.addJSD: Starting.')

        self.JSD.add('grid_resource', 'deltacloud %s' % self.gridresource) 

        self.JSD.add("deltacloud_username", "%s" % self.username) 
        self.JSD.add("deltacloud_password_file", "%s" % self.password_file) 

        if self.image_id:
            self.JSD.add('deltacloud_image_id', '%s' % self.image_id)          
        if self.keyname:
            self.JSD.add('deltacloud_keyname', '%s' % self.keyname)          
        if self.realm_id:
            self.JSD.add('delta_realm_id', '%s' %self.realm_id)
        if self.hardware_profile:
            self.JSD.add('delta_hardware_profile', '%s' %self.hardware_profile)
        if self.hardware_profile_memory:
            self.JSD.add('delta_hardware_profile_memory', '%s' %self.hardware_profile_memory)
        if self.hardware_profile_cpu:
            self.JSD.add('delta_hardware_profile_cpu', '%s' %self.hardware_profile_cpu)
        if self.hardware_profile_storage:
            self.JSD.add('delta_hardware_profile_storage', '%s' %self.hardware_profile_storage)
        if self.user_data:
            self.JSD.add('delta_user_data', '%s' %self.user_data)

        super(CondorDeltaCloudBatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorDeltaCloudBatchSubmitPlugin.addJSD: Leaving.')


