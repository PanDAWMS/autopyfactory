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
<h1><a name="Glidein_style_workflows_with_Aut"></a>  Glidein style workflows with AutoPyFactory </h1>
<li> 1  About this Document</a>
</li> <li> 2  Applicable versions</a>
</li> <li> 3  Configuration</a>
</li> <li> 4  Example 1</a>
</li> <li> 5  Example 2</a>
</li></ul> 
<p />
<h1><a name="1_About_this_Document"></a> 1  About this Document </h1>
<p />
This document explains how to setup <a href="../index.html" class="twikiLink">AutoPyFactory</a> to work in a glidein style fashion
<p />
<p />
<p />
<h1><a name="2_Applicable_versions"></a> 2  Applicable versions </h1>
<p />
This documentation applies to the latest version of APF: 2.4.9
<p />
<p />
<h1><a name="3_Configuration"></a> 3  Configuration </h1>
<p />
In the case of workflows using glideins, the payloads are condor jobs submitted to a pool, usually with no startd's associated. 
Then, when the factory notices those IDLE jobs in the condor pool, submits pilots (glideins in this case) to remote resources. These glideins will be configured to join the pool and allow those IDLE jobs to finally run. 
In this scenario, in order to allow the APF factory to see the IDLE jobs in the condor pool, they need to be submitted with an specific classad:
<p />
<pre class="file">
+MATCH_APF_QUEUE=&lt;label&gt;
</pre>
<p />
This line will typically be included in the condor description file being used to submit the payload jobs to the condor pool. 
The particular string <code>+MATCH_APF_QUEUE</code> tells the AutoPyFactory that those jobs are to be managed by the factory. 
The different values of the <code>label</code> will allow the factory to treat them differently. All payload jobs with the same <code>label</code> will be managed together. Payload jobs with different <code>label</code> will be managed separately. 
<p />
As it was explained <a href="../AutoPyFactoryConfiguration/" target="_top">here</a>, and APFQueue can be interpreted as the unique combination of a wms queue and a batch queue. in this particular type of workflow, the value of the <code>+MATCH_APF_QUEUE</code> classad represents the wms queue. 
<p />
Then, the batch queue, the system where the pilots will be submitted to, can be, for example, a remote Compute Element, or another condor pool. 
<p />
<h1><a name="4_Example_1"></a> 4  Example 1 </h1>
<p />
Let's say some of the payload jobs have been submitted to the empty pool by the end user with classad:
<p />
<pre class="file">
+MATCH_APF_QUEUE=montecarlo
</pre>
<p />
This first type of jobs are meant to be run in a remote grid site. 
<p />
In this case, the <code>WMSStatus</code> plugin is <code>Condor</code>, as the condor pool is our WMS service in this case. 
As the glideins will be submitted to a remote grid site -for example with a Globus GT5 gatekeeper, the <code>BatchSubmitPlugin</code> is <code>CondorGT5</code>. 
To track the status of previously submitted glideins, as they are being submitted with condor-g, the <code>BatchStatusPlugin</code> is <code>Condor</code>.
<p />
So the basic configuration for this workflow in the <code>queues.conf</code> file would include a section similar to this:
<p />
<pre class="file">
[MC_SITE1_CE]
wmsqueue = montecarlo
wmsstatusplugin = Condor
batchstatusplugin = Condor
batchsubmitplugin = CondorGT5
...
</pre>
<p />
For the rest of variables, see the <a href="../AutoPyFactoryReferenceManual/" target="_top">Reference Manual</a>
<p />
<p />
<h1><a name="5_Example_2"></a> 5  Example 2 </h1>
<p />
In this case, the end user payload jobs have classad:
<p />
<pre class="file">
+MATCH_APF_QUEUE=analysis
</pre>
<p />
This second type of jobs are meant to run on a local condor cluster. 
In that case, the value of the <code>CondorSubmitPlugin</code> is <code>CondorLocal</code>. It is called "local" even if the schedd for this pool is in a remote host. 
So the  <code>queues.conf</code> file would have a section like this:
<p />
<pre class="file">
[ANALYSIS_CONDORPOOL_XYZ]
wmsqueue = analysis
wmsstatusplugin = Condor
batchstatusplugin = Condor
batchsubmitplugin = CondorLocal
batchsubmit.condorlocal.submitargs = -remote &lt;remote_schedd&gt;
...
</pre>
<p />
For the rest of variables, see the <a href="../AutoPyFactoryReferenceManual/" target="_top">Reference Manual</a>
<p /> 


</body></html>
