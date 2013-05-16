#!/bin/env python
#
# Small script to test Panda data retrieval and format.
#

import userinterface.Client as Client
import pprint

def testcloud():
    print("Testing cloud retrieval:")
    clouds_err, all_clouds_config = Client.getCloudSpecs()
    print("dir all_clouds_config: %s" % dir(all_clouds_config))
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(all_clouds_config)
    print("###########################################")

def testsites():
    print("Testing site retrieval:")
    sites_err, all_sites_config = Client.getSiteSpecs(siteType='all')
    print("dir all_sites_config %s" % dir(all_sites_config))
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(all_sites_config)
    print("###########################################")

def testjobs():
    print("Testing job retrieval:")
    jobs_err, all_jobs_config = Client.getJobStatisticsPerSite(
                        countryGroup='',
                        workingGroup='', 
                        jobType='test,prod,managed,user,panda,ddm,rc_test'
                        )  
    print("dir all_jobs_config %s" % dir(all_jobs_config))
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(all_jobs_config)
    print("###########################################")

def testjobswithlabelswithlabels():
    print("Testing job retrieval:")
    jobs_err, all_jobs_config = Client.getJobStatisticsWithLabel()
    print("dir all_jobs_config %s" % dir(all_jobs_config))
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(all_jobs_config)
    print("###########################################")


def testpanda():
    print("Testing all...")
    testcloud()
    testsites()
    testjobs()
    testjobswithlabelswithlabels()
    
if __name__=='__main__':
    testpanda()
