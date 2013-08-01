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


# Start Giraffa fs daemons.

# TODO 
# usage="Usage: start-grfa.sh [--config confdir] [--hadoop hadoopdir] [--hbase hbasedir] [-upgrade|-rollback]"

usage="Usage: start-grfa.sh [-upgrade|-rollback]"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
cd $bin/..
pwd

. "$bin"/giraffa-config.sh

# get arguments
if [ $# -ge 1 ]; then
	nameStartOpt=$1
	shift
	case $nameStartOpt in
	  (-upgrade)
	  	;;
	  (-rollback) 
	  	dataStartOpt=$nameStartOpt
	  	;;
	  (*)
		  echo $usage
		  exit 1
	    ;;
	esac
fi

# start hdfs
# "$HADOOP_HOME"/sbin/start-dfs.sh $nameStartOpt

echo "HADOOP_HOME = " $HADOOP_HOME

cd "$HADOOP_HOME" 
. "$HADOOP_HOME"/libexec/hdfs-config.sh

echo "starting namenode"
"$HADOOP_HOME"/sbin/hadoop-daemon.sh start namenode $nameStartOpt

echo "starting datanode"
"$HADOOP_HOME"/sbin/hadoop-daemon.sh start datanode $dataStartOpt

# start hbase
echo "HBASE_HOME = " $HBASE_HOME

cd "$HBASE_HOME" 
echo "starting hbase"
HADOOP_HOME=
. "$HBASE_HOME"/bin/start-hbase.sh
