#
#

import logging
import logging.handlers
import sys
import time
import unittest

from autopyfactory.plugins.queue.batchstatus.Condor import MockQueue, getMockQueueConfig, Condor


class TestCondorStatus(unittest.TestCase):

    def test_condor_status(self):   
        mq = MockQueue()
        cp = getMockQueueConfig()
        #   (apfqueue, config, section    
        cbs = Condor(mq,cp,'mock') 
        cbs._updatejobinfo()
        cbs._updatelib()
    
        info = cbs.getInfo()
        jobinfo = cbs.getJobInfo()
        print(info)
        #print(jobinfo)
    





