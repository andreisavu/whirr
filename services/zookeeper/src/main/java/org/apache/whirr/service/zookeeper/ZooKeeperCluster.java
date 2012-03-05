/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.service.zookeeper;

import com.google.common.base.Function;
import com.google.common.base.Joiner;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.whirr.Cluster;
import static org.apache.whirr.RolePredicates.role;

public class ZooKeeperCluster {

  public static String getHosts(Cluster cluster, String roleName, int clientPort, boolean internalHosts) {
    return Joiner.on(',').join(
        getHosts(cluster.getInstancesMatching(role(roleName)), clientPort, internalHosts)
    );
  }

  public static String getHosts(Cluster cluster, String roleName, int clientPort) {
    return getHosts(cluster, roleName, clientPort, false);
  }

  public static List<String> getPrivateIps(Set<Cluster.Instance> instances) {
    return Lists.transform(Lists.newArrayList(instances),
        new Function<Cluster.Instance, String>() {
          @Override
          public String apply(Cluster.Instance instance) {
            return instance.getPrivateIp();
          }
        });
  }

  public static List<String> getPublicHosts(Set<Cluster.Instance> instances, int clientPort) {
    return getHosts(instances, clientPort, false);
  }

  public static List<String> getHosts(Set<Cluster.Instance> instances, final int clientPort, final boolean internalHost) {
    return Lists.transform(Lists.newArrayList(instances),
        new Function<Cluster.Instance, String>() {
          @Override
          public String apply(Cluster.Instance instance) {
            try {
              String host;
              if (internalHost) {
                host = instance.getPrivateHostName();
              } else {
                host = instance.getPublicHostName();
              }
              return String.format("%s:%d", host, clientPort);
            } catch (IOException e) {
              throw new IllegalArgumentException(e);
            }
          }
        });
  }
}
