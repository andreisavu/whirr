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

package org.apache.whirr;

import com.google.common.base.Function;
import org.apache.whirr.actions.ByonClusterAction;
import org.apache.whirr.service.ComputeCache;
import org.apache.whirr.state.ClusterStateStore;
import org.apache.whirr.state.ClusterStateStoreFactory;
import org.jclouds.compute.ComputeServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import static org.apache.whirr.service.ClusterActionHandler.BOOTSTRAP_ACTION;
import static org.apache.whirr.service.ClusterActionHandler.CLEANUP_ACTION;
import static org.apache.whirr.service.ClusterActionHandler.CONFIGURE_ACTION;
import static org.apache.whirr.service.ClusterActionHandler.START_ACTION;
import static org.apache.whirr.service.ClusterActionHandler.STOP_ACTION;

/**
 * Equivalent of {@link ClusterController}, but for execution in BYON mode
 * ("bring your own nodes").
 */
public class ByonClusterController extends ClusterController {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterController.class);

  public ByonClusterController() {
    super(ComputeCache.INSTANCE, new ClusterStateStoreFactory());
  }

  public ByonClusterController(Function<ClusterSpec, ComputeServiceContext> getCompute,
      ClusterStateStoreFactory stateStoreFactory) {
    super(getCompute, stateStoreFactory);
  }

  @Override
  public String getName() {
    return "byon";
  }

  @Override
  public Cluster bootstrapCluster(ClusterSpec clusterSpec) throws IOException, InterruptedException {
    ClusterAction bootstrapper = new ByonClusterAction(BOOTSTRAP_ACTION, getCompute(), createHandlerMap());
    return bootstrapper.execute(clusterSpec, null);
  }

  @Override
  public Cluster configureServices(ClusterSpec clusterSpec, Cluster cluster,
      Set<String> targetRoles, Set<String> targetInstanceIds) throws IOException, InterruptedException {
    ClusterAction configurer = new ByonClusterAction(CONFIGURE_ACTION, getCompute(), createHandlerMap());
    return configurer.execute(clusterSpec, cluster);
  }

  @Override
  public Cluster startServices(ClusterSpec clusterSpec, Cluster cluster,
      Set<String> targetRoles, Set<String> targetInstanceIds) throws IOException, InterruptedException {
    ClusterAction configurer = new ByonClusterAction(START_ACTION, getCompute(), createHandlerMap());
    return configurer.execute(clusterSpec, cluster);
  }

  @Override
  public Cluster stopServices(ClusterSpec clusterSpec, Cluster cluster,
      Set<String> targetRoles, Set<String> targetInstanceIds) throws IOException, InterruptedException {
    ClusterAction configurer = new ByonClusterAction(STOP_ACTION, getCompute(), createHandlerMap());
    return configurer.execute(clusterSpec, cluster);
  }

  @Override
  public Cluster cleanupCluster(ClusterSpec clusterSpec, Cluster cluster) throws IOException, InterruptedException {
    ClusterAction configurer = new ByonClusterAction(CLEANUP_ACTION, getCompute(), createHandlerMap());
    return configurer.execute(clusterSpec, cluster);
  }

  @Override
  public void destroyCluster(ClusterSpec clusterSpec, Cluster cluster, ClusterStateStore stateStore)
      throws IOException, InterruptedException {
    // TODO: what does it mean destroy for byon?
  }

  @Override
  public void destroyInstance(ClusterSpec clusterSpec, String instanceId)
      throws IOException {
    // TODO
  }
}
