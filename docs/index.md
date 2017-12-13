</div>
<br>
<br>
<br>
<br>
<br>
<br>
<br>
<br>
<br>
<br>
<h1><a name="AutoPyFactory"></a> AutoPyFactory </h1>
<li> 1  About this Document</a>
</li> <li> 2  Applicable versions</a>
</li> <li> 3  Description</a> <ul>
<li> 3.1  Plugins-based architecture</a> <ul>
<li> 3.1.1 WMS Status plugin</a>
</li> <li> 3.1.2  Batch Status plugin</a>
</li> <li> 3.1.3  Scheduler plugin</a>
</li> <li> 3.1.4  Batch Submit plugin</a>
</li></ul> 
</li></ul> 
</li> <li> 4  Deployment and Configuration</a>
</li> <li> 5  Development</a>
</li> <li> 6  AutoPyFactory Tools</a>
</li> <li> 7  Questions and answers</a>
</li> <li> 8  Contact</a>
</li> <li> 9  Talks and Publications</a>
</li></ul> 
<p />
<h1><a name="1_About_this_Document"></a> 1  About this Document </h1>
<p />
This document describes what is AutoPyFactory (a.k.a. APF)
<p />
<p />
<p />
<h1><a name="2_Applicable_versions"></a> 2  Applicable versions </h1>
<p />
This documentation applies to the latest version of APF: 2.4.9
<p />
<p />
<h1><a name="3_Description"></a> 3  Description </h1>
<p />
ATLAS, one of the experiments at LHC at CERN, is one of the largest users of grid computing infrastructure. As this infrastructure is now a central part of the experiment's computing operations, considerable efforts have been made to use this technology in the most efficient and effective way, including extensive use of pilot job based frameworks.
<p />
In this model the experiment submits 'pilot' jobs to sites without payload. When these jobs begin to run they contact a central service to pick-up a real payload to execute.
<p />
The first generation of pilot factories were usually specific to a single VO, and were very bound to the particular architecture of that VO. A second generation is creating factories which are more flexible, not tied to any particular VO, and provide for more features other than just pilot submission (such as monitoring, logging, profiling, etc.)
<p />
AutoPyFactory has a modular design and is highly configurable. It is able to send different types of pilots to sites, able to exploit different submission mechanisms and different characteristics of queues at sites. It has excellent integration with the PanDA job submission framework, tying pilot flows closely to the amount of work the site has to run. It is able to gather information from many sources, in order to correctly configure itself for a site and its decision logic can easily be updated.
<p />
Integrated into AutoPyFactory is a very flexible system for delivering both generic and specific wrappers which can perform many useful actions before starting to run end-user scientific applications, e.g., validation of the middleware, node profiling and diagnostics, monitoring and deciding what is the best end-user application that fits the resource.
<p />
AutoPyFactory now also has a robust monitoring system and we show how this has helped setup a reliable pilot factory service for ATLAS.
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="3_1_Plugins_based_architecture"></a> 3.1  Plugins-based architecture </span></h2>
<p />
AutoPyFactory can serve to different queues in different ways thanks to its modular design basedon plug-ins. 
Plug-ins serve two purposes. 
They interact with the external services, like the VO WMS or the batch submission system, 
and they translate the information retrieved by those services into the internal AutoPyFactory nomenclature.
<p />
Some of the most important plugins in AutoPyFactory are described below.
<p />
<h3><a name="3_1_1_WMS_Status_plugin"></a> 3.1.1  WMS Status plugin </h3>
<p />
Queries the VO WMS system, retrieving information about the number of jobs in different status (ready, running, finished...)  per queue.  
This information is converted internally into the AutoPyFactory nomenclature. 
An example of a WMS Status plug-in queries the PanDA API. 
Another example is a plug-in querying a local Condor pool and interpreting the output as end-user jobs. 
This source of information is typically where how much work is ready to be done can be found, 
and therefore should trigger pilot submission.
<p />
<h3><a name="3_1_2_Batch_Status_plugin"></a> 3.1.2  Batch Status plugin </h3>
<p />
<p />
Queries the batch system being used to submit the jobs (or pilots) to the grid resources, 
to determine how many previously submitted jobs are already being executed and how many are still idle.  
This information is used to avoid submitting an unnecessary number of extra jobs, 
which could cause bottlenecks, inefficiencies, and even impose severe loads on remote Grid services. 
An example is a module querying the Condor queues.
<p />
<h3><a name="3_1_3_Scheduler_plugin"></a> 3.1.3  Scheduler plugin </h3>
<p />
This is the component in charge of making a decision of whether or not to submit more pilots, and if so how many. 
That calculation is based on the information provided by the two Status plug-ins (WMSStatus and BatchStatus).  
It implements a given algorithm to decide how many new jobs (or pilots) should be submitted next cycle. 
A typical algorithm calculates the number of new jobs based on the number of end-user jobs in a ready
status in the VO WMS service, with some constraints to prevent the submission of an excessively
high number of jobs, or to eventually keep a minimum number of submissions per cycle.
Other SchedPlugins may embody other algorithms, e.g. a scheduler plug-in could always return a fixed number of jobs, 
or one could seek to maintain a constant number of pending/queued jobs in the batch system.
<p />
More than one scheduler plugins can be used combined, where the output of each one is the input for the next one in the chain.
<p />
<h3><a name="3_1_4_Batch_Submit_plugin"></a> 3.1.4  Batch Submit plugin </h3>
It is the component in charge of submitting new jobs (or pilots),
based on the decision made by the Scheduler plug-in. 
Examples of these execution plug-ins can submit jobs remotely to a Grid resource using different protocols 
(such as GRAM2, GRAM5, or CREAM), 
to a Cloud Computing resource (using the Amazon EC2 protocol), or to a local Condor pool.
<p />
In theory, a submit plug-in could use other mechanisms, 
e.g. simply execute a pilot process,
or trigger an additional VM startup locally via libvirtd. 
In this scenario, AutoPyFactory could be run directly on the working resource (wherever the jobs are intended to run).
<p />
<p />
<h1><a name="4_Deployment_and_Configuration"></a> 4  Deployment and Configuration </h1>
<p /> <ul>
<li> Instructions to deploy AutoPyFactory are <a href="AutoPyFactoryDeployment/" target="_top">here</a>
</li> <li> Instructions to configure AutoPyFactory can be found <a href="AutoPyFactoryConfiguration/" target="_top">here</a>
</li> <li> Reference manual to configure AutoPyFactory are <a href="AutoPyFactoryReferenceManual/" target="_top">here</a>
</li></ul> 
<p />
<h1><a name="5_Development"></a> 5  Development </h1>
<p />
Notes on how to write new plugins can be found <a href="AutoPyFactoryWritePlugins/" target="_top">here</a>
<p />
<h1><a name="6_AutoPyFactory_Tools"></a> 6 AutoPyFactory Tools </h1>
<p />
Documentation on a set of utils around AutoPyFactory can be found <a href="AutoPyFactoryTools/" target="_top">here</a>
<p />
<p />
<h1><a name="7_Questions_and_answers"></a> 7  Questions and answers </h1>
<p />
There is a <a href="AutoPyFactoryQA/" target="_top">Q&amp;A</a> page with some questions from users and the answer.
<p />
<h1><a name="8_Contact"></a> 8  Contact </h1>
<p />
There is a mailing list, where new releases are announced and users can post questions. <br>
To join follow <a href="https://lists.bnl.gov/mailman/listinfo/autopyfactory-l" target="_top">instructions here</a>
<p />
<h1><a name="9_Talks_and_Publications"></a> 9  Talks and Publications </h1>
<p />
<a href="https://indico.cern.ch/getFile.py/access?contribId=15&amp;sessionId=11&amp;resId=0&amp;materialId=slides&amp;confId=119171" target="_top">Talk at ATLAS S&amp;C Week, 17 October 2011 to 21 October 2011</a>
<p />
<a href="https://indico.cern.ch/contributionDisplay.py?contribId=329&amp;sessionId=8&amp;confId=149557" target="_top">Poster at CHEP 2012, NY</a>
<p />
<a href="http://iopscience.iop.org/1742-6596/396/3/032016" target="_top">Paper at CHEP 2012, NY</a>
<p />
<a href="https://indico.fnal.gov/getFile.py/access?contribId=41&amp;sessionId=10&amp;resId=0&amp;materialId=slides&amp;confId=5610" target="_top">Talk at Open Science Grid All Hands Meeting, 2013</a>
<p />
<a href="https://indico.cern.ch/contributionDisplay.py?confId=214784&amp;contribId=280" target="_top">Poster at CHEP 2013, Amsterdam</a>
<p />
<a href="https://indico.cern.ch/event/438205/contributions/2205047/attachments/1291816/1924624/APF_Update_ADC_TIM_June_2016-1.pdf" target="_top">Talk at the TIM at CERN, June 2016</a>

</body>
</html>
