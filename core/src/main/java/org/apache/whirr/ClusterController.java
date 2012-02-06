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
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.state.ClusterStateStore;
import org.apache.whirr.state.ClusterStateStoreFactory;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeState;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.whirr.RolePredicates.withIds;
import static org.jclouds.compute.options.RunScriptOptions.Builder.overrideCredentialsWith;

public abstract class ClusterController {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterController.class);

  private static final ImmutableSet<String> EMPTYSET = ImmutableSet.of();

  private final HandlerMapFactory handlerMapFactory = new HandlerMapFactory();
  private final Function<ClusterSpec, ComputeServiceContext> getCompute;
  private final ClusterStateStoreFactory stateStoreFactory;

  public ClusterController(Function<ClusterSpec, ComputeServiceContext> getCompute,
                           ClusterStateStoreFactory stateStoreFactory) {
    this.getCompute = getCompute;
    this.stateStoreFactory = stateStoreFactory;
  }

  /**
   * @return compute service contexts for use in managing the service
   */
  protected Function<ClusterSpec, ComputeServiceContext> getCompute() {
    return getCompute;
  }

  protected Map<String, ClusterActionHandler> createHandlerMap() {
    return handlerMapFactory.create();
  }

  protected ClusterStateStore getClusterStateStore(ClusterSpec clusterSpec) {
    return stateStoreFactory.create(clusterSpec);
  }

  public HandlerMapFactory getHandlerMapFactory() {
    return handlerMapFactory;
  }

  /**
   * @return the unique name of the service.
   */
  public String getName() {
    throw new UnsupportedOperationException("No service name");
  }

  /**
   * @see ClusterController#launchCluster
   */
  public Cluster launchCluster(ClusterSpec clusterSpec)
      throws IOException, InterruptedException {
    try {
      Cluster cluster = bootstrapCluster(clusterSpec);
      cluster = configureServices(clusterSpec, cluster);
      return startServices(clusterSpec, cluster);

    } catch (Throwable e) {

      if (clusterSpec.isTerminateAllOnLaunchFailure()) {
        LOG.error("Unable to start the cluster. Terminating all nodes.", e);
        destroyCluster(clusterSpec);

      } else {
        LOG.error("*CRITICAL* the cluster failed to launch and the automated node termination" +
            " option was not selected, there might be orphaned nodes.", e);
      }

      throw new RuntimeException(e);
    }
  }

  /**
   * Bootstrap cluster by provisioning nodes and running the
   * service installation scripts
   *
   * @param clusterSpec
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract Cluster bootstrapCluster(ClusterSpec clusterSpec) throws IOException, InterruptedException;

  /**
   * Configure services on all or just a subset of the cluster nodes
   *
   * @param clusterSpec
   * @param cluster
   * @param targetRoles
   * @param targetInstanceIds
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract Cluster configureServices(ClusterSpec clusterSpec, Cluster cluster, Set<String> targetRoles,
    Set<String> targetInstanceIds) throws IOException, InterruptedException;

  public Cluster configureServices(ClusterSpec spec) throws IOException, InterruptedException {
    return configureServices(spec, new Cluster(getInstances(spec, getClusterStateStore(spec))));
  }

  public Cluster configureServices(ClusterSpec clusterSpec, Cluster cluster)
      throws IOException, InterruptedException {
    return configureServices(clusterSpec, cluster, EMPTYSET, EMPTYSET);
  }

  /**
   * Start services on all or just a subset of the cluster nodes
   *
   * @param clusterSpec
   * @param cluster
   * @param targetRoles
   * @param targetInstanceIds
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract Cluster startServices(ClusterSpec clusterSpec, Cluster cluster,
    Set<String> targetRoles, Set<String> targetInstanceIds) throws IOException, InterruptedException;

  public Cluster startServices(ClusterSpec spec) throws IOException, InterruptedException {
    return startServices(spec, new Cluster(getInstances(spec, getClusterStateStore(spec))));
  }

  public Cluster startServices(ClusterSpec clusterSpec, Cluster cluster)
      throws IOException, InterruptedException {
    return startServices(clusterSpec, cluster, EMPTYSET, EMPTYSET);
  }

  /**
   * Stop cluster services on all or just a subset of the cluster nodes
   *
   * @param clusterSpec
   * @param cluster
   * @param targetRoles
   * @param targetInstanceIds
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract Cluster stopServices(ClusterSpec clusterSpec, Cluster cluster, Set<String> targetRoles,
    Set<String> targetInstanceIds) throws IOException, InterruptedException;

  public Cluster stopServices(ClusterSpec spec) throws IOException, InterruptedException {
    return stopServices(spec, new Cluster(getInstances(spec, getClusterStateStore(spec))));
  }

  public Cluster stopServices(ClusterSpec clusterSpec, Cluster cluster)
      throws IOException, InterruptedException {
    return stopServices(clusterSpec, cluster, EMPTYSET, EMPTYSET);
  }

  /**
   * Remove services from cluster nodes
   *
   * @param clusterSpec
   * @param cluster
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract Cluster cleanupCluster(ClusterSpec clusterSpec, Cluster cluster)
      throws IOException, InterruptedException;

  public Cluster cleanupCluster(ClusterSpec spec) throws IOException, InterruptedException {
    return cleanupCluster(spec, new Cluster(getInstances(spec, getClusterStateStore(spec))));
  }

  public Map<? extends NodeMetadata, ExecResponse> runScriptOnNodesMatching(ClusterSpec spec,
      Predicate<NodeMetadata> condition, Statement statement) throws IOException, RunScriptOnNodesException {
    return runScriptOnNodesMatching(spec, condition, statement, null);
  }

  /**
   * Destroy the cluster
   *
   * @param clusterSpec
   * @param cluster
   * @param stateStore
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract void destroyCluster(ClusterSpec clusterSpec, Cluster cluster, ClusterStateStore stateStore)
      throws IOException, InterruptedException;

  public void destroyCluster(ClusterSpec clusterSpec) throws IOException, InterruptedException {
    ClusterStateStore stateStore = getClusterStateStore(clusterSpec);
    destroyCluster(clusterSpec, stateStore.tryLoadOrEmpty(), stateStore);
  }

  /**
   * Destroy a single isntance from a running cluster
   *
   * @param clusterSpec
   * @param instanceId
   * @throws IOException
   */
  public abstract void destroyInstance(ClusterSpec clusterSpec, String instanceId) throws IOException;

  public Map<? extends NodeMetadata, ExecResponse> runScriptOnNodesMatching(
      ClusterSpec spec, Predicate<NodeMetadata> condition, Statement statement,
      RunScriptOptions options) throws IOException, RunScriptOnNodesException {

    Credentials credentials = new Credentials(spec.getClusterUser(),
        spec.getPrivateKey());

    if (options == null) {
      options = defaultRunScriptOptionsForSpec(spec);
    } else if (options.getOverridingCredentials() == null) {
      options = options.overrideCredentialsWith(credentials);
    }
    condition = Predicates
        .and(runningInGroup(spec.getClusterName()), condition);

    ComputeServiceContext context = getCompute().apply(spec);
    return context.getComputeService().runScriptOnNodesMatching(condition,
        statement, options);
  }

  public RunScriptOptions defaultRunScriptOptionsForSpec(ClusterSpec spec) {
    Credentials credentials = new Credentials(spec.getClusterUser(),
        spec.getPrivateKey());
    return overrideCredentialsWith(credentials).wrapInInitScript(false)
        .runAsRoot(false);
  }

  @Deprecated
  public Set<? extends NodeMetadata> getNodes(ClusterSpec clusterSpec)
      throws IOException, InterruptedException {
    ComputeService computeService = getCompute().apply(clusterSpec).getComputeService();
    return computeService.listNodesDetailsMatching(
        runningInGroup(clusterSpec.getClusterName()));
  }

  public Set<Cluster.Instance> getInstances(ClusterSpec spec)
      throws IOException, InterruptedException {
    return getInstances(spec, null);
  }

  public Set<Cluster.Instance> getInstances(ClusterSpec spec, ClusterStateStore stateStore)
      throws IOException, InterruptedException {

    Set<Cluster.Instance> instances = Sets.newLinkedHashSet();
    Cluster cluster = (stateStore != null) ? stateStore.load() : null;

    for (NodeMetadata node : getNodes(spec)) {
      instances.add(toInstance(node, cluster, spec));
    }

    return instances;
  }

  private Cluster.Instance toInstance(NodeMetadata metadata, Cluster cluster, ClusterSpec spec) {
    Credentials credentials = new Credentials(spec.getClusterUser(), spec.getPrivateKey());

    Set<String> roles = Sets.newHashSet();
    try {
      if (cluster != null) {
        roles = cluster.getInstanceMatching(withIds(metadata.getId())).getRoles();
      }
    } catch (NoSuchElementException e) {
    }

    return new Cluster.Instance(credentials, roles,
        Iterables.getFirst(metadata.getPublicAddresses(), null),
        Iterables.getFirst(metadata.getPrivateAddresses(), null),
        metadata.getId(), metadata);
  }

  public static Predicate<ComputeMetadata> runningInGroup(final String group) {
    return new Predicate<ComputeMetadata>() {
      @Override
      public boolean apply(ComputeMetadata computeMetadata) {
        // Not all list calls return NodeMetadata (e.g. VCloud)
        if (computeMetadata instanceof NodeMetadata) {
          NodeMetadata nodeMetadata = (NodeMetadata) computeMetadata;
          return group.equals(nodeMetadata.getGroup())
              && nodeMetadata.getState() == NodeState.RUNNING;
        }
        return false;
      }

      @Override
      public String toString() {
        return "runningInGroup(" + group + ")";
      }
    };
  }

}
