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
<p />
<h1><a name="Submission_to_CREAM_CE"></a>  Submission to CREAM CE </h1>

<li> 1  About this Document</a>
</li> <li> 2  Applicable versions</a>
</li> <li> 3  Configuration</a>
</li></ul> 

<p />
<h1><a name="1_About_this_Document"></a> 1  About this Document </h1>
<p />
This document describes how to configure AutoPyFactory to submit to a CREAM CE.
<p />
<p />
<h1><a name="2_Applicable_versions"></a> 2  Applicable versions </h1>
<p />
This documentation applies to the latest version of APF: 2.4.9
<p />
<p />
<h1><a name="3_Configuration"></a> 3  Configuration </h1>
<p />
In order to submit to CREAM, just set accordingly the batch submit plugin, and all related attributes. 
For example:
<p />
<pre class="file">
batchsubmitplugin = CondorCREAM

batchsubmit.condorcream.webservice = lcgce04.gridpp.rl.ac.uk
batchsubmit.condorcream.port = 8443
batchsubmit.condorcream.batch = pbs
batchsubmit.condorcream.queue = grid3000M 
batchsubmit.condorcream.condor_attributes = periodic_remove = (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > 3600) || (JobStatus == 1 && globusstatus =!= 1 && (CurrentTime - EnteredCurrentStatus) > 86400)

batchsubmit.condorcream.proxy = atlas-production
</pre>
<p />


</body></html>
