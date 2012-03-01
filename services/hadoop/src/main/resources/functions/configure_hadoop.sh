#
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
#
function configure_hadoop() {
  local OPTIND
  local OPTARG

  if [ -d /data/hadoop ]; then
    echo "Hadoop is already configured."
    return;
  fi

  ROLES=$1
  shift
  
  case $CLOUD_PROVIDER in
    ec2 | aws-ec2 )
      # Alias /mnt as /data
      ln -s /mnt /data
      ;;
    *)
      ;;
  esac
  
  HADOOP_HOME=/usr/local/hadoop
  HADOOP_CONF_DIR=$HADOOP_HOME/conf

  mkdir -p /data/hadoop
  chown hadoop:hadoop /data/hadoop
  if [ ! -e /data/tmp ]; then
    mkdir /data/tmp
    chmod a+rwxt /data/tmp
  fi
  mkdir /etc/hadoop
  ln -s $HADOOP_CONF_DIR /etc/hadoop/conf

  # Copy generated configuration files in place
  cp /tmp/{core,hdfs,mapred}-site.xml $HADOOP_CONF_DIR
  cp /tmp/hadoop-env.sh $HADOOP_CONF_DIR

  # Keep PID files in a non-temporary directory
  HADOOP_PID_DIR=$(. /tmp/hadoop-env.sh; echo $HADOOP_PID_DIR)
  HADOOP_PID_DIR=${HADOOP_PID_DIR:-/var/run/hadoop}
  mkdir -p $HADOOP_PID_DIR
  chown -R hadoop:hadoop $HADOOP_PID_DIR

  # Create the actual log dir
  mkdir -p /data/hadoop/logs
  chown -R hadoop:hadoop /data/hadoop/logs

  # Create a symlink at $HADOOP_LOG_DIR
  HADOOP_LOG_DIR=$(. /tmp/hadoop-env.sh; echo $HADOOP_LOG_DIR)
  HADOOP_LOG_DIR=${HADOOP_LOG_DIR:-/var/log/hadoop/logs}
  rm -rf $HADOOP_LOG_DIR
  mkdir -p $(dirname $HADOOP_LOG_DIR)
  ln -s /data/hadoop/logs $HADOOP_LOG_DIR
  chown -R hadoop:hadoop $HADOOP_LOG_DIR
}


