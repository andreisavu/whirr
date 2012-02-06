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
import org.apache.whirr.actions.BootstrapClusterAction;
import org.apache.whirr.actions.CleanupClusterAction;
import org.apache.whirr.actions.ConfigureServicesAction;
import org.apache.whirr.actions.DestroyClusterAction;
import org.apache.whirr.actions.StartServicesAction;
import org.apache.whirr.actions.StopServicesAction;
import org.apache.whirr.service.ComputeCache;
import org.apache.whirr.state.ClusterStateStore;
import org.apache.whirr.state.ClusterStateStoreFactory;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import static org.apache.whirr.RolePredicates.withIds;

/**
 * This class is used to start and stop clusters.
 */
public class CloudClusterController extends ClusterController {

  private static final Logger LOG = LoggerFactory.getLogger(CloudClusterController.class);

  public CloudClusterController() {
    super(ComputeCache.INSTANCE, new ClusterStateStoreFactory());
  }

  public CloudClusterController(Function<ClusterSpec, ComputeServiceContext> getCompute,
                                ClusterStateStoreFactory stateStoreFactory) {
    super(getCompute, stateStoreFactory);
  }

  /**
   * @see ClusterController#bootstrapCluster
   */
  @Override
  public Cluster bootstrapCluster(ClusterSpec clusterSpec) throws IOException, InterruptedException {
    BootstrapClusterAction bootstrapper = new BootstrapClusterAction(getCompute(), createHandlerMap());
    Cluster cluster = bootstrapper.execute(clusterSpec, null);
    getClusterStateStore(clusterSpec).save(cluster);
    return cluster;
  }

  /**
   * @see ClusterController#configureServices
   */
  @Override
  public Cluster configureServices(ClusterSpec clusterSpec, Cluster cluster, Set<String> targetRoles,
        Set<String> targetInstanceIds) throws IOException, InterruptedException {
    ConfigureServicesAction configurer = new ConfigureServicesAction(getCompute(), createHandlerMap(),
        targetRoles, targetInstanceIds);
    return configurer.execute(clusterSpec, cluster);
  }

  /**
   * @see ClusterController#startServices
   */
  @Override
  public Cluster startServices(ClusterSpec clusterSpec, Cluster cluster,
      Set<String> targetRoles, Set<String> targetInstanceIds) throws IOException, InterruptedException {
    StartServicesAction starter = new StartServicesAction(getCompute(),
        createHandlerMap(), targetRoles, targetInstanceIds);
    return starter.execute(clusterSpec, cluster);
  }

  /**
   * @see ClusterController#stopServices
   */
  @Override
  public Cluster stopServices(ClusterSpec clusterSpec, Cluster cluster, Set<String> targetRoles,
    Set<String> targetInstanceIds) throws IOException, InterruptedException {
    StopServicesAction stopper = new StopServicesAction(getCompute(),
        createHandlerMap(), targetRoles, targetInstanceIds);
    return stopper.execute(clusterSpec, cluster);
  }

  /**
   * @see ClusterController#cleanupCluster
   */
  @Override
  public Cluster cleanupCluster(ClusterSpec clusterSpec, Cluster cluster)
    throws IOException, InterruptedException {
    CleanupClusterAction cleanner = new CleanupClusterAction(getCompute(), createHandlerMap());
    return cleanner.execute(clusterSpec, cluster);
  }

  /**
   * @see ClusterController#destroyCluster
   */
  @Override
  public void destroyCluster(ClusterSpec clusterSpec, Cluster cluster, ClusterStateStore stateStore)
    throws IOException, InterruptedException {
    DestroyClusterAction destroyer = new DestroyClusterAction(getCompute(), createHandlerMap());
    destroyer.execute(clusterSpec, cluster);
    stateStore.destroy();
  }

  /**
   * @see ClusterController#destroyInstance
   */
  @Override
  public void destroyInstance(ClusterSpec clusterSpec, String instanceId) throws IOException {
    LOG.info("Destroying instance {}", instanceId);

    /* Destroy the instance */
    ComputeService computeService = getCompute().apply(clusterSpec).getComputeService();
    computeService.destroyNode(instanceId);

    /* .. and update the cluster state storage */
    ClusterStateStore store = getClusterStateStore(clusterSpec);
    Cluster cluster = store.load();
    cluster.removeInstancesMatching(withIds(instanceId));
    store.save(cluster);

    LOG.info("Instance {} destroyed", instanceId);
  }
}
