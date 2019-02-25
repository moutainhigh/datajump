package com.ai.ds.dj.config;

import com.ai.ds.dj.config.protocol.HostAndPort;

import java.util.ArrayList;
import java.util.List;

/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */
public class ToNodeConfig {

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public List<HostAndPort> getNode() {
        return node;
    }

    public void setNode(List<HostAndPort> node) {
        this.node = node;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getAuth() {
        return auth;
    }

    public void setAuth(String auth) {
        this.auth = auth;
    }

    private String clusterName;
  private List<HostAndPort> node = new ArrayList<HostAndPort>();
  private int timeout;
  private String auth;
}
