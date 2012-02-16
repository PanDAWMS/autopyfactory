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
        '''
        This class is expected to have separate instances for each PandaQueue object. 
        '''
        
        def __init__(self, apfqueue, qcl=None):

                self.id = 'condorec2'

                # Chosing the queue config object, depending on 
                # it was an input option or not.
                #       If it was passed as input option, then that is the config object. 
                #       If not, then it is extracted from the apfqueue object
                if not qcl:
                    qcl = apfqueue.factory.qcl

                # we rename the queue config variables to pass a new config object to parent class
                newqcl = qcl.clone().filterkeys('batchsubmit.condorec2', 'batchsubmit.condorgrid')
                super(CondorEC2BatchSubmitPlugin, self).__init__(apfqueue, newqcl)

                try:
                        self.gridresource = qcl.get(self.apfqname, 'batchsubmit.condorec2.gridresource') 

                        self.ami_id = qcl.get(self.apfqname, 'batchsubmit.condorec2.ami_id')
                        self.instance_type  = qcl.get(self.apfqname, 'batchsubmit.condorec2.instance_type')
                        self.user_data = None        
                        if qcl.has_option(self.apfqname, 'batchsubmit.condorec2.user_data'):
                                self.user_data = qcl.get(self.apfqname, 'batchsubmit.condorec2.user_data')
                        #self.x509userproxy = self.factory.proxymanager.getProxyPath(qcl.get(self.apfqname,'batchsubmit.condorec2.proxy'))
                        self.access_key_id = qcl.get(self.apfqname,'batchsubmit.condorec2.access_key_id')
                        self.secret_access_key = qcl.get(self.apfqname,'batchsubmit.condorec2.secret_access_key')

                        self.log.info('CondorEC2BatchSubmitPlugin: Object initialized.')
                except:         
                        self._valid = False

        def _addJSD(self):

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


