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

package org.apache.whirr.service.cdh.hadoop;

import java.io.IOException;
import org.apache.commons.configuration.Configuration;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import static org.jclouds.scriptbuilder.domain.Statements.call;

public abstract class CdhClusterActionHandler extends ClusterActionHandlerSupport {

  protected Configuration getConfiguration(ClusterSpec clusterSpec) throws IOException {
    return getConfiguration(clusterSpec, "whirr-cdh-default.properties");
  }

  protected String getInstallFunction(Configuration config) {
    return getInstallFunction(config, "hadoop", "install_cdh_hadoop");
  }

  protected String getConfigureFunction(Configuration config) {
    return getConfigureFunction(config, "hadoop", "configure_cdh_hadoop");
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Configuration conf = getConfiguration(clusterSpec);

    addStatement(event, call("configure_hostnames"));

    addStatement(event, call(getInstallFunction(conf, "java", "install_openjdk")));
    addStatement(event, call("install_tarball"));

    String tarball = prepareRemoteFileUrl(event, conf.getString("whirr.hadoop.tarball.url"));

    addStatement(event, call(getInstallFunction(conf), "-u", tarball));
  }

}
