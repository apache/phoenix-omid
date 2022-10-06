
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
# OMID Changelog

## Release 1.1.0 - Unreleased (as of 2022-10-06)



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [OMID-231](https://issues.apache.org/jira/browse/OMID-231) | Build and test Omid with Hadoop 3 |  Major | . |
| [OMID-232](https://issues.apache.org/jira/browse/OMID-232) | Do not depend on netty-all |  Major | . |
| [OMID-223](https://issues.apache.org/jira/browse/OMID-223) | Refactor Omid to use HBase 2 APIs internally |  Major | . |
| [OMID-222](https://issues.apache.org/jira/browse/OMID-222) | Remove HBase1 support and update HBase 2 version to 2.4 |  Major | . |
| [OMID-221](https://issues.apache.org/jira/browse/OMID-221) | Bump junit from 4.13 to 4.13.1 |  Major | . |
| [OMID-220](https://issues.apache.org/jira/browse/OMID-220) | Update netty to 4.1.76.Final |  Major | . |
| [OMID-209](https://issues.apache.org/jira/browse/OMID-209) | Migrate to commons-lang3 |  Major | . |
| [OMID-202](https://issues.apache.org/jira/browse/OMID-202) | Refactor Omid to use Netty 4 |  Major | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [OMID-189](https://issues.apache.org/jira/browse/OMID-189) | Fix RAT check errors |  Major | . |
| [OMID-230](https://issues.apache.org/jira/browse/OMID-230) | Update Netty and commons-io versions |  Major | . |
| [OMID-229](https://issues.apache.org/jira/browse/OMID-229) | Allow only "squash and merge" from GitHub UI |  Major | . |
| [OMID-224](https://issues.apache.org/jira/browse/OMID-224) | Switch default logging backend to log4j2 |  Major | . |
| [OMID-216](https://issues.apache.org/jira/browse/OMID-216) | Remove log4j.properties from maven artifact |  Major | . |
| [OMID-211](https://issues.apache.org/jira/browse/OMID-211) | HBase Shims leak testing dependencies as compile dependencies. |  Major | . |
| [OMID-210](https://issues.apache.org/jira/browse/OMID-210) | Build failure on Linux ARM64 |  Major | . |
| [OMID-198](https://issues.apache.org/jira/browse/OMID-198) | Replace static ports used for TSO server with random ports in the tests |  Major | . |
| [OMID-200](https://issues.apache.org/jira/browse/OMID-200) | Omid client cannot use kerberos cache when using proxyUser |  Blocker | . |
| [OMID-199](https://issues.apache.org/jira/browse/OMID-199) | Omid client cannot use pre-authenticated UserGroupInformation.getCurrentUser() |  Blocker | . |
| [OMID-197](https://issues.apache.org/jira/browse/OMID-197) | Replace Mockito timeout#never() with times(0) |  Major | . |
| [OMID-196](https://issues.apache.org/jira/browse/OMID-196) | Add junit test dependency for modules having minicluster tests |  Major | . |
| [OMID-194](https://issues.apache.org/jira/browse/OMID-194) | OmidTableManager cannot create  commit and timestamp tables in kerberos cluster |  Blocker | . |
| [OMID-188](https://issues.apache.org/jira/browse/OMID-188) | Fix "inconsistent module metadata found" when using hbase-2 |  Major | . |
| [OMID-192](https://issues.apache.org/jira/browse/OMID-192) | fix missing jcommander dependency |  Blocker | . |
| [OMID-191](https://issues.apache.org/jira/browse/OMID-191) | Fix missing executable permission because of MASSEMBLY-941 |  Blocker | . |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [OMID-208](https://issues.apache.org/jira/browse/OMID-208) | Pass additional options to omid.sh |  Major | . |
| [OMID-226](https://issues.apache.org/jira/browse/OMID-226) | Migrate from mockito-all to mockito-core and clean up test dependencies |  Major | . |
| [OMID-233](https://issues.apache.org/jira/browse/OMID-233) | Fix license check |  Major | . |
| [OMID-227](https://issues.apache.org/jira/browse/OMID-227) | Upgrade jcommander |  Minor | . |
| [OMID-228](https://issues.apache.org/jira/browse/OMID-228) | Upgrade snakeyaml |  Minor | . |
| [OMID-219](https://issues.apache.org/jira/browse/OMID-219) | Update to phoenix-thirdparty 2.0 |  Major | . |
| [OMID-218](https://issues.apache.org/jira/browse/OMID-218) | Update OWASP plugin to latest |  Major | . |
| [OMID-214](https://issues.apache.org/jira/browse/OMID-214) | Upgrade commons-io to 2.11.0 |  Major | . |
| [OMID-207](https://issues.apache.org/jira/browse/OMID-207) | Upgrade to snakeyaml 1.26 |  Major | . |
| [OMID-193](https://issues.apache.org/jira/browse/OMID-193) | Upgrade netty version to latest 3.x |  Major | . |



## Release 1.0.2 - Unreleased (as of 2020-11-17)



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [OMID-165](https://issues.apache.org/jira/browse/OMID-165) | Adopt Omid packaging to Phoenix standards |  Major | . |
| [OMID-168](https://issues.apache.org/jira/browse/OMID-168) | cleanup the example code using HBase 0.x and yahoo imports |  Minor | . |
| [OMID-120](https://issues.apache.org/jira/browse/OMID-120) | Utilize protobuf-maven-plugin for build |  Major | . |
| [OMID-164](https://issues.apache.org/jira/browse/OMID-164) | Update phoenix-thirdparty dependency version to 1.0.0 |  Major | . |
| [OMID-161](https://issues.apache.org/jira/browse/OMID-161) | Switch default timestampType to WORLD\_TIME |  Major | . |
| [OMID-158](https://issues.apache.org/jira/browse/OMID-158) | Add OWASP dependency check, and update the flagged direct dependencies |  Major | . |
| [OMID-156](https://issues.apache.org/jira/browse/OMID-156) | refactor Omid to use phoenix-shaded-guava |  Major | . |
| [OMID-155](https://issues.apache.org/jira/browse/OMID-155) | disable trimStackTrace |  Major | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [OMID-187](https://issues.apache.org/jira/browse/OMID-187) | Fix incorrect URL of source code |  Trivial | . |
| [OMID-166](https://issues.apache.org/jira/browse/OMID-166) | AbstractTransactionManager.closeResources should be public |  Major | . |
| [OMID-159](https://issues.apache.org/jira/browse/OMID-159) | Replace  default hbase commit table and timestamp modules in server configurations as for the new package structure |  Major | . |
| [OMID-157](https://issues.apache.org/jira/browse/OMID-157) | Multiple problems with Travis tests |  Major | . |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [OMID-162](https://issues.apache.org/jira/browse/OMID-162) | Remove org.mortbay.log.Log and add junit dependency |  Major | . |
| [OMID-160](https://issues.apache.org/jira/browse/OMID-160) | Remove -incubating from assembly name |  Major | . |


