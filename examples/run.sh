#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# ---------------------------------------------------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------------------------------------------------

function show_help() {

    echo "
    ###################################################################################################################
    #
    # This script allows to run small examples showing Omid functionality and APIs. These are the examples provided:
    #
    # 1) basic -> Executes a transaction encompassing a multi-row modification in HBase
    # 2) si -> Shows how Omid preserves Snapshot Isolation guarantees when concurrent transactions access shared data
    # 3) conf -> Basic example showing Omid client configuration options
    #
    # See the source-code to get more details about each example.
    #
    # The Omid examples can be executed with the following command pattern:
    #
    # $ $0 <example> [ tablename ] [column family name]
    #
    # where:
    # - <example> can be [ basic | si | conf | parallel ]
    #
    # Example execution:
    # ------------------
    #
    # $0 basic
    #
    # will run the basic example with the default options
    #
    # $ $0 basic myHBaseTable myCF
    #
    # will run the basic example using 'myHBaseTable' as table name and 'myCF' as column family.
    # All Omid client-related configuration settings assume their default values. The default values for the settings
    # can be found in 'omid-client-config.yml' and 'default-hbase-omid-client-config.yml' files in the source code.
    #
    ###################################################################################################################
    "

}

fileNotFound(){
    echo "omid-examples.jar not found!";
    exit 1;
}

# ---------------------------------------------------------------------------------------------------------------------
# --------------------------------------------- SCRIPT STARTS HERE ----------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------

SCRIPTDIR=`dirname $0`
cd $SCRIPTDIR;
SCRIPTDIR=`pwd`

# ---------------------------------------------------------------------------------------------------------------------
# Check if HADOOP_CONF_DIR and HBASE_CONF_DIR are set
# ---------------------------------------------------------------------------------------------------------------------

if [ -z ${HADOOP_CONF_DIR+x} ]; then echo "WARNING: HADOOP_CONF_DIR is unset"; else echo "HADOOP_CONF_DIR is set to '$HADOOP_CONF_DIR'"; fi
if [ -z ${HBASE_CONF_DIR+x} ]; then echo "WARNING: HBASE_CONF_DIR is unset"; else echo "HBASE_CONF_DIR is set to '$HBASE_CONF_DIR'"; fi

# ---------------------------------------------------------------------------------------------------------------------
# Check if jar exists and configure classpath
# ---------------------------------------------------------------------------------------------------------------------

[ ! -f "omid-examples.jar" ] && fileNotFound

KLASSPATH=omid-examples.jar:${HBASE_CONF_DIR}:${HADOOP_CONF_DIR}:${SCRIPTDIR}/conf

for jar in ./lib/*.jar; do
    if [ -f "$jar" ]; then
        KLASSPATH=$KLASSPATH:$jar
    fi
done

# ---------------------------------------------------------------------------------------------------------------------
# Parse CLI user args
# ---------------------------------------------------------------------------------------------------------------------

USER_OPTION=$1
shift
case ${USER_OPTION} in
    basic)
        java -cp $KLASSPATH -Dlog4j2.configurationFile=file:${SCRIPTDIR}/conf/log4j2.properties org.apache.omid.examples.BasicExample "$@"
        ;;
    si)
        java -cp $KLASSPATH -Dlog4j2.configurationFile=file:${SCRIPTDIR}/conf/log4j2.properties org.apache.omid.examples.SnapshotIsolationExample "$@"
        ;;
    parallel)
        java -cp $KLASSPATH -Dlog4j2.configurationFile=file:${SCRIPTDIR}/conf/log4j2.properties org.apache.omid.examples.ParallelExecution "$@"
        ;;
    conf)
        java -cp $KLASSPATH -Dlog4j2.configurationFile=file:${SCRIPTDIR}/conf/log4j2.properties org.apache.omid.examples.ConfigurationExample "$@"
        ;;
    *)
        show_help
        ;;
esac
