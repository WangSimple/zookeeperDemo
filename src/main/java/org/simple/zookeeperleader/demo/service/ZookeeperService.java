package org.simple.zookeeperleader.demo.service;

import java.util.List;

/**
 * @description:
 * @author: xpwang
 * @date: 2020-12-23 4:58 下午
 */
public interface ZookeeperService {
    /**
     * 获取节点数据
     */
    String getNodeData (String path) ;
    /**
     * 获取节点下数据
     */
    List<String> getNodeChild (String path) ;
}
