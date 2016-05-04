#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

from CondorGridBatchSubmitPlugin import CondorGridBatchSubmitPlugin


class CondorDeltaCloudBatchSubmitPlugin(CondorGridBatchSubmitPlugin):
    id = 'condordeltacloud'
    
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.factory.qcl            
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

        self.classads['GridResource'] = 'deltacloud %s' % self.gridresource
        self.classads['DeltacloudUsername'] = self.username
        self.classads['DeltacloudPasswordFile'] = self.password_file

        if self.image_id:
            self.classads['DeltacloudImageId'] = self.image_id
        if self.keyname:
            self.classads['DeltacloudKeyname'] = self.keyname
        if self.realm_id:
            self.classads['DeltaRealmId'] = self.realm_id
        if self.hardware_profile:
            self.classads['DeltaHardwareProfile'] = self.hardware_profile
        if self.hardware_profile_memory:
            self.classads['DeltaHardwareProfileMemory'] = self.hardware_profile_memory
        if self.hardware_profile_cpu:
            self.classads['DeltaHardwareProfileCpu'] = self.hardware_profile_cpu
        if self.hardware_profile_storage:
            self.classads['DeltaHardwareProfileStorage'] = self.hardware_profile_storage
        if self.user_data:
            self.classads['DeltacloudUserData'] = self.user_data

        super(CondorDeltaCloudBatchSubmitPlugin, self)._addJSD()

        self.log.debug('CondorDeltaCloudBatchSubmitPlugin.addJSD: Leaving.')


