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
<h1><a name="Submission_to_EC2"></a>  Submission to EC2 </h1>
<li> 1  About this Document</a>
</li> <li> 2  Applicable versions</a>
</li> <li> 3  Configuration</a>
</li></ul> 
<p />
<h1><a name="1_About_this_Document"></a> 1  About this Document </h1>
<p />
This document describes how to configure AutoPyFactory to submit to Amazon EC2
<p />
<p />
<h1><a name="2_Applicable_versions"></a> 2  Applicable versions </h1>
<p />
This documentation applies to the latest version of APF: 2.4.9
<p />
<p />
<h1><a name="3_Configuration"></a> 3  Configuration </h1>
<p />
In order to submit to EC2, just set accordingly the batch status and submit plugins, and all related attributes. 
For example:
<p />
<pre class="file">
batchstatusplugin = CondorEC2
batchsubmitplugin = CondorEC2

batchsubmit.condorec2.gridresource = https://ec2.us-west-1.amazonaws.com/

batchsubmit.condorec2.access_key_id = /home/autopyfactory/etc/ec2-racf-cloud/access.key
batchsubmit.condorec2.secret_access_key = /home/autopyfactory/etc/ec2-racf-cloud/secret.key
batchsubmit.condorec2.security_groups = foo
batchsubmit.condorec2.usessh = False
batchsubmit.condorec2.peaceful = False
batchsubmit.condorec2.ami_id = ami-abc123
batchsubmit.condorec2.instance_type = m2.4xlarge
batchsubmit.condorec2.spot_price = 0.25

</pre>
<p />


</body></html>
