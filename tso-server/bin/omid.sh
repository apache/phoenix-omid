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

SCRIPTDIR=`dirname $0`
cd $SCRIPTDIR;

# Load Omid environment variables
source omid-env.sh

# Configure classpath...
CLASSPATH=../conf:${HBASE_CONF_DIR}:${HADOOP_CONF_DIR}

# ...for source release and...
for j in ../target/omid-tso*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

# and for binary release
for j in ../omid-tso*.jar; do
    CLASSPATH=$CLASSPATH:$j
done
for j in ../lib/*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

#JVM detection and list of JDK11 options copied from HBase with slight modifications

function read_java_version() {
  # Avoid calling java repeatedly
  if [ -z "$read_java_version_cached" ]; then
    properties="$("${JAVA_HOME}/bin/java" -XshowSettings:properties -version 2>&1)"
    read_java_version_cached="$(echo "${properties}" | "${GREP}" java.runtime.version | head -1 | "${SED}" -e 's/.* = \([^ ]*\)/\1/')"
  fi
  echo "$read_java_version_cached"
}

# Inspect the system properties exposed by this JVM to identify the major
# version number. Normalize on the popular version number, thus consider JDK
# 1.8 as version "8".
function parse_java_major_version() {
  complete_version=$1
  # split off suffix version info like '-b10' or '+10' or '_10'
  # careful to not use GNU Sed extensions
  version="$(echo "$complete_version" | "${SED}" -e 's/+/_/g' -e 's/-/_/g' | cut -d'_' -f1)"
  case "$version" in
  1.*)
    echo "$version" | cut -d'.' -f2
    ;;
  *)
    echo "$version" | cut -d'.' -f1
    ;;
  esac
}

add_jdk11_jvm_flags() {
  # Keep in sync with omid-surefire.jdk11.flags in the root pom.xml
  OMID_OPTS="$OMID_OPTS -Dorg.apache.hbase.thirdparty.io.netty.tryReflectionSetAccessible=true --add-modules jdk.unsupported --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-exports java.base/sun.net.dns=ALL-UNNAMED --add-exports java.base/sun.net.util=ALL-UNNAMED"
}

setup_jdk_options()  {

  # We don't actually add any JARs, but this is copied from HBase which does
  addJDK11Jars=false

  if [ "${OMID_JDK11}" != "" ]; then
    # Use the passed Environment Variable HBASE_JDK11
    if [ "${OMID_JDK11}" = "include" ]; then
      addJDK11Jars=true
      if [ "${DEBUG}" = "true" ]; then
        echo "OMID_JDK11 set as 'include' hence adding JDK11 jars to classpath."
      fi
    elif [ "${OMID_JDK11}" = "exclude" ]; then
      if [ "${DEBUG}" = "true" ]; then
        echo "OMID_JDK11 set as 'exclude' hence skipping JDK11 jars to classpath."
      fi
    else
      echo "[OMID_JDK11] contains unsupported value(s) - ${HBASE_JDK11}. Ignoring passed value."
      echo "[OMID_JDK11] supported values: [include, exclude]."
    fi
  else
    # Use JDK detection
    export GREP="${GREP-grep}"
    export SED="${SED-sed}"
    version="$(read_java_version)"
    major_version_number="$(parse_java_major_version "$version")"

    if [ "${DEBUG}" = "true" ]; then
      echo "OMID_JDK11 not set hence using JDK detection."
      echo "Extracted JDK version - ${version}, major_version_number - ${major_version_number}"
    fi

    if [[ "$major_version_number" -ge "11" ]]; then
      if [ "${DEBUG}" = "true" ]; then
        echo "Version ${version} is greater-than/equal to 11 hence adding JDK11 jars to classpath."
      fi
      addJDK11Jars=true
    elif [ "${DEBUG}" = "true" ]; then
        echo "Version ${version} is lesser than 11 hence skipping JDK11 jars from classpath."
    fi
  fi

  if [ "${addJDK11Jars}" = "true" ]; then
    add_jdk11_jvm_flags
    if [ "${DEBUG}" = "true" ]; then
       echo "Added JDK11 JVM flags."
    fi
  elif [ "${DEBUG}" = "true" ]; then
    echo "Skipped adding JDK11 JVM flags."
  fi
}

tso() {
    exec java $JVM_FLAGS $OMID_OPTS -cp $CLASSPATH org.apache.omid.tso.TSOServer $@
}

tsoRelauncher() {
    until ./omid.sh tso $@; do
        echo "TSO Server crashed with exit code $?.  Re-launching..." >&2
        sleep 1
    done
}

createHBaseCommitTable() {
    exec java $OMID_OPTS -cp $CLASSPATH org.apache.omid.tools.hbase.OmidTableManager commit-table $@
}

createHBaseTimestampTable() {
    exec java $OMID_OPTS -cp $CLASSPATH org.apache.omid.tools.hbase.OmidTableManager timestamp-table $@
}

usage() {
    echo "Usage: omid.sh <command> <options>"
    echo "where <command> is one of:"
    echo "  tso                           Starts The Status Oracle server (TSO)"
    echo "  tso-relauncher                Starts The Status Oracle server (TSO) re-launching it if the process exits"
    echo "  create-hbase-commit-table     Creates the hbase commit table."
    echo "  create-hbase-timestamp-table  Creates the hbase timestamp table."
}

# if no args specified, show usage
if [ $# = 0 ]; then
    usage;
    exit 1
fi

setup_jdk_options

COMMAND=$1
shift

if [ "$COMMAND" = "tso" ]; then
    tso $@;
elif [ "$COMMAND" = "tso-relauncher" ]; then
    tsoRelauncher $@;
elif [ "$COMMAND" = "create-hbase-commit-table" ]; then
    createHBaseCommitTable $@;
elif [ "$COMMAND" = "create-hbase-timestamp-table" ]; then
    createHBaseTimestampTable $@;
else
    exec java -cp $CLASSPATH $COMMAND $@
fi


