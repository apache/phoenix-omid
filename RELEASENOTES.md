
<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->
# OMID  1.1.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [OMID-247](https://issues.apache.org/jira/browse/OMID-247) | *Critical* | **Change TSO default port to be outside the ephemeral range**

The default port for the TSO server has been changed from 54758 to 24758 to avoid random TSO server startup failures due to conflicts with ephemeral TCP ports in use at the host.
When updating existing installations make sure that the port is the same in the omid client and server config files: either update the port in the omid client config files for all clients, or revert to the old port on the server side.



# OMID  1.1.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [OMID-231](https://issues.apache.org/jira/browse/OMID-231) | *Major* | **Build and test Omid with Hadoop 3**

Omid is now built with HBase 2.x and Hadoop 3.1.

As the public binary HBase 2.x artifacts don't work with Hadoop3, you need to download the HBase sources, and rebuild and install them to the local maven repo before building Omid.

The easiest way to this is to run the following command line:

dev-support/rebuild\_hbase.sh detect

This will download the hbase version used by Omid, rebuild it, and install into the local maven repo.


---

* [OMID-224](https://issues.apache.org/jira/browse/OMID-224) | *Major* | **Switch default logging backend to log4j2**

Omid has switched to to the log4j2 logging backend.
The code was already using slf4j, and backend-agonistic, but now the utilities and binary assembly are using log4j2 instead of log4j.
The default logging configuration has also been changes to more closely match the HBase logging config.


---

* [OMID-222](https://issues.apache.org/jira/browse/OMID-222) | *Major* | **Remove HBase1 support and update HBase 2 version to 2.4**

Omid no longer supports HBase 1.x
As a result of this, the build process and artifact structure has ben simplified, there are no longer two sets of artifacts to be built.
Users need to remove the -hbase2.x postfix from the artifact names, and remove any omid shim dependencies from the project as those have been completely removed.


---

* [OMID-188](https://issues.apache.org/jira/browse/OMID-188) | *Major* | **Fix "inconsistent module metadata found" when using hbase-2**

The maven artifact structure has changed.

Every artifact is built is built separately for HBase 1 and 2. You will need to add the -hbase-1.x or -hbase-2.x suffix to every Omid artifact that you depend on.

You no longer need to specify exclusions when using the HBase 2.x artifacts.



# OMID  1.0.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [OMID-161](https://issues.apache.org/jira/browse/OMID-161) | *Major* | **Switch default timestampType to WORLD\_TIME**

The default timestampType has been changed from INCREMENTAL to  WORLD\_TIME.
To restore the 1.0.1 behaviour add the "timestampType: INCREMENTAL" line to omid-server-configuration.yml.



