package org.apache.whirr.service.cdh.hadoop;

import org.apache.whirr.service.ClusterActionHandlerSupport;

public class CdhDataNodeClusterActionHandler extends ClusterActionHandlerSupport {

  @Override
  public String getRole() {
    return "cdh-datanode";
  }

}
