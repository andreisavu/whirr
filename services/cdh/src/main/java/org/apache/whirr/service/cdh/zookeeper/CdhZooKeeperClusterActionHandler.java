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

package org.apache.whirr.service.cdh.zookeeper;

import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import static org.apache.whirr.RolePredicates.role;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.FirewallManager;
import org.apache.whirr.service.zookeeper.ZooKeeperCluster;
import static org.jclouds.scriptbuilder.domain.Statements.call;

public class CdhZooKeeperClusterActionHandler extends ClusterActionHandlerSupport {

  public static final String ROLE = "cdh-zookeeper";
  public static final int CLIENT_PORT = 2181;

  @Override
  public String getRole() {
    return ROLE;
  }

  protected Configuration getConfiguration(ClusterSpec spec) throws IOException {
    return getConfiguration(spec, "whirr-cdh-zookeeper-default.properties");
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    installJDK(event);
    addStatement(event, call(getInstallFunction(getConfiguration(event.getClusterSpec()),
        "install_cdh_zookeeper")));
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    event.getFirewallManager().addRule(
        FirewallManager.Rule.create().destination(role(ROLE)).port(CLIENT_PORT)
    );

    // Pass list of all servers in ensemble to configure script.
    // Position is significant: i-th server has id i.

    Set<Cluster.Instance> ensemble = cluster.getInstancesMatching(role(ROLE));
    String servers = Joiner.on(' ').join(ZooKeeperCluster.getPrivateIps(ensemble));

    Configuration config = getConfiguration(clusterSpec);
    addStatement(event, call(getConfigureFunction(config, "configure_cdh_zookeeper"), servers));
  }
}
