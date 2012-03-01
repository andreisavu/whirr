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

function start_hadoop_namenode() {
  source /etc/profile

  if which dpkg &> /dev/null; then
    AS_HADOOP="su -s /bin/bash - hadoop -c"
  elif which rpm &> /dev/null; then
    AS_HADOOP="/sbin/runuser -s /bin/bash - hadoop -c"
  fi

  # Format HDFS
  FIRST_RUN_AFTER_CONFIGURE=0
  if [ ! -e /data/hadoop/hdfs ]; then
    $AS_HADOOP "$HADOOP_HOME/bin/hadoop namenode -format"
    FIRST_RUN_AFTER_CONFIGURE=1
  fi

  $AS_HADOOP "$HADOOP_HOME/bin/hadoop-daemon.sh start namenode"

  if [ "$FIRST_RUN_AFTER_CONFIGURE" == "1" ]; then
    $AS_HADOOP "$HADOOP_HOME/bin/hadoop dfsadmin -safemode wait"
    $AS_HADOOP "$HADOOP_HOME/bin/hadoop fs -mkdir /user"
    # The following is questionable, as it allows a user to delete another user
    # It's needed to allow users to create their own user directories
    $AS_HADOOP "$HADOOP_HOME/bin/hadoop fs -chmod +w /user"

    # Create temporary directory for Pig and Hive in HDFS
    $AS_HADOOP "$HADOOP_HOME/bin/hadoop fs -mkdir /tmp"
    $AS_HADOOP "$HADOOP_HOME/bin/hadoop fs -chmod +w /tmp"
    $AS_HADOOP "$HADOOP_HOME/bin/hadoop fs -mkdir /user/hive/warehouse"
    $AS_HADOOP "$HADOOP_HOME/bin/hadoop fs -chmod +w /user/hive/warehouse"
  fi
}
