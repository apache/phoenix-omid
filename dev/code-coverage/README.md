<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Clover code analysis

The `run-coverage.sh` script runs maven with the code-coverage profile which generates
the clover code analysis data.
If the necessary parameters are given it also uploads the results to SonarQube.

## Running code coverage

The coverage results can be found under `target/clover/index.html` and here is how you can run the clover code analysis:

```sh dev-support/code-coverage/run-coverage.sh```

## Publishing coverage results to SonarQube

The required parameters for publishing to SonarQube are:

- host URL,
- login credentials,
- project key

The project name is an optional parameter.

Here is an example command for running and publishing the coverage data:

`sh dev-support/code-coverage/run-coverage.sh -l ProjectCredentials -u https://exampleserver.com
-k Project_Key -n Project_Name`