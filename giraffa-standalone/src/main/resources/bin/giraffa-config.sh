# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# included in all the hadoop scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# Resolve links ($0 may be a softlink) and convert a relative path
# to an absolute path.  NB: The -P option requires bash built-ins
# or POSIX:2001 compliant cd and pwd.
this="${BASH_SOURCE-$0}"
giraffa_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$giraffa_bin/$script"

# the root of the Giraffa installation
export GIRAFFA_HOME=`dirname "$this"`/..

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
    then
	shift
	confdir=$1
	shift
	GIRAFFA_CONF_DIR=$confdir
    fi
fi

# Allow alternate conf dir location.
export GIRAFFA_CONF_DIR="${GIRAFFA_CONF_DIR:-$GIRAFFA_HOME/conf}"

cygwin=false
case "`uname`" in
CYGWIN*) cygwin=true;;
esac

if [ -f "${GIRAFFA_CONF_DIR}/giraffa-env.sh" ]; then
  . "${GIRAFFA_CONF_DIR}/giraffa-env.sh"
fi

# Java parameters
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx1000m

# check envvars which might override default args
if [ "$GIRAFFA_HEAPSIZE" != "" ]; then
  #echo "run with heapsize $GIRAFFA_HEAPSIZE"
  JAVA_HEAP_MAX="-Xmx""$GIRAFFA_HEAPSIZE""m"
  #echo $JAVA_HEAP_MAX
fi

# CLASSPATH initially contains $GIRAFFA_CONF_DIR
CLASSPATH="${GIRAFFA_CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

# Enable filenames w/ spaces
IFS=

# for developers, add Hadoop classes to CLASSPATH
if [ -d "$GIRAFFA_HOME/target/classes" ]; then
  GIRAFFA_CLASSPATH=${GIRAFFA_CLASSPATH}:$GIRAFFA_HOME/target/classes
fi
if [ -d "$GIRAFFA_HOME/target/test-classes" ]; then
  GIRAFFA_CLASSPATH=${GIRAFFA_CLASSPATH}:$GIRAFFA_HOME/target/test-classes
fi

if [ -d "${GIRAFFA_HOME}/lib" ]; then
  for f in $GIRAFFA_HOME/lib/*.jar; do
    if [[ "$f" != *"slf4j-log4j"* ]] || [[ "$1" == "format" ]]; then
      GIRAFFA_CLASSPATH=${GIRAFFA_CLASSPATH}:$f;
    fi
  done
fi
export GIRAFFA_CLASSPATH=$GIRAFFA_CLASSPATH
# echo $GIRAFFA_CLASSPATH

# add user-specified CLASSPATH last
if [ "$GIRAFFA_CLASSPATH" != "" ]; then
  CLASSPATH=${CLASSPATH}:${GIRAFFA_CLASSPATH}
fi
# echo $CLASSPATH

# default log directory & file
if [ "$GIRAFFA_LOG_DIR" = "" ]; then
  GIRAFFA_LOG_DIR="$GIRAFFA_HOME/logs"
fi
if [ "$GIRAFFA_LOGFILE" = "" ]; then
  GIRAFFA_LOGFILE='giraffa.log'
fi

GIRAFFA_OPTS="$GIRAFFA_OPTS -Dgiraffa.log.dir=$GIRAFFA_LOG_DIR"
GIRAFFA_OPTS="$GIRAFFA_OPTS -Dgiraffa.log.file=$GIRAFFA_LOGFILE"
GIRAFFA_OPTS="$GIRAFFA_OPTS -Dgiraffa.root.logger=${GIRAFFA_ROOT_LOGGER:-INFO,RFA}"
GIRAFFA_OPTS="$GIRAFFA_OPTS -Dgiraffa.security.logger=${GIRAFFA_SECURITY_LOGGER:-INFO,NullAppender}"
if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
  GIRAFFA_OPTS="$GIRAFFA_OPTS -Djava.library.path=$JAVA_LIBRARY_PATH"
fi

# Disable ipv6 as it can cause issues
GIRAFFA_OPTS="$GIRAFFA_OPTS -Djava.net.preferIPv4Stack=true"

# set hadoop home if present
if [ "$HADOOP_HOME" = "" ]; then
  if [ -d "${GIRAFFA_HOME}/../hadoop" ]; then
    HADOOP_HOME=$GIRAFFA_HOME/../hadoop
  fi
fi

# cygwin path translation
if $cygwin; then
  HADOOP_HOME=`cygpath -p "$HADOOP_HOME"`
fi

# set hbase home if present
if [ "$HBASE_HOME" = "" ]; then
  if [ -d "${GIRAFFA_HOME}/../hbase" ]; then
    HBASE_HOME=$GIRAFFA_HOME/../hbase
  fi
fi

# cygwin path translation
if $cygwin; then
  HBASE_HOME=`cygpath -p "$HBASE_HOME"`
fi

# cygwin path translation
if $cygwin; then
  GIRAFFA_HOME=`cygpath -p "$GIRAFFA_HOME"`
  GIRAFFA_LOG_DIR=`cygpath -p "$GIRAFFA_LOG_DIR"`
fi
