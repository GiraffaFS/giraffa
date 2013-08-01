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


# Stop Giraffa fs daemons.

# TODO
# usage="Usage: stop-grfa.sh [--config confdir] [--hadoop hadoopdir] [--hbase hbasedir]"

usage="Usage: stop-grfa.sh"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "$bin"/giraffa-config.sh

# get arguments

# stop hdfs
# "$HADOOP_HOME"/sbin/stop-dfs.sh

echo "HADOOP_HOME = " $HADOOP_HOME

cd "$HADOOP_HOME" 
. "$HADOOP_HOME"/libexec/hdfs-config.sh

echo "stopping namenode"
"$HADOOP_HOME"/sbin/hadoop-daemon.sh --script "$bin"/hdfs stop namenode

echo "stopping datanode"
"$HADOOP_HOME"/sbin/hadoop-daemon.sh --script "$bin"/hdfs stop datanode

# start hbase
echo "stopping hbase"
HADOOP_HOME=
"$HBASE_HOME"/bin/stop-hbase.sh
