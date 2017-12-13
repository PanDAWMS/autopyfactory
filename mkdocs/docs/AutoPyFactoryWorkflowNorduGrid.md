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
<h1><a name="Submission_to_NorduGrid_CE"></a>  Submission to NorduGrid CE </h1>
<li> 1  About this Document</a>
</li> <li> 2  Applicable versions</a>
</li> <li> 3  Configuration</a>
</li></ul> 
<p />
<h1><a name="1_About_this_Document"></a> 1  About this Document </h1>
<p />
This document describes how to configure AutoPyFactory to submit to NorduGrid
<p />
<p />
<h1><a name="2_Applicable_versions"></a> 2  Applicable versions </h1>
<p />
This documentation applies to the latest version of APF: 2.4.9
<p />
<p />
<h1><a name="3_Configuration"></a> 3  Configuration </h1>
<p />
In order to submit to ND, just set accordingly the batch submit plugin, and all related attributes. 
For example:
<p />
<pre class="file">
batchsubmitplugin = CondorNordugrid

batchsubmit.condornordugrid.gridresource = lcg-lrz-ce2.grid.lrz.de 

nordugridrsl.jobname = 'analy_pilot'
nordugridrsl.queue = lcg
nordugridrsl.nordugridrsladd = (runtimeenvironment = APPS/HEP/ATLAS-SITE-LCG)(runtimeenvironment = ENV/PROXY )
nordugridrsl.addenv.RUCIO_ACCOUNT = pilot
</pre>
<p />


</body></html>
