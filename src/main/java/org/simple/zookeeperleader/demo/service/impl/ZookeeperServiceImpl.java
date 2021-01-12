package org.simple.zookeeperleader.demo.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.simple.zookeeperleader.demo.service.ZookeeperService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @description:
 * @author: xpwang
 * @date: 2020-12-23 5:00 下午
 */
@Slf4j
@Component
public class ZookeeperServiceImpl implements ZookeeperService {

    @Autowired
    private CuratorFramework curatorFramework;

    @Override
    public String getNodeData(String path) {
        try {
            // 数据读取和转换
            byte[] dataByte = curatorFramework.getData().forPath(path) ;
            String data = new String(dataByte,"UTF-8") ;
            if (StringUtils.isNotEmpty(data)){
                return data ;
            }
        }catch (Exception e) {
            log.error("getNodeData error...", e);
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<String> getNodeChild(String path) {
        List<String> nodeChildDataList = new ArrayList<>();
        try {
            // 节点下数据集
            nodeChildDataList = curatorFramework.getChildren().forPath(path);
        } catch (Exception e) {
            log.error("getNodeChild error...", e);
            e.printStackTrace();
        }
        return nodeChildDataList;
    }


    public void watcherOnce() throws Exception {
        Stat stat = new Stat();

        String path = "/server/address";

        // 回调函数
        Watcher watcher = new Watcher() {

            @Override
            public void process(WatchedEvent event) {

                System.err.println("zookeeper的配置信息发生改变了");

            }
        };
        curatorFramework.getZookeeperClient().getZooKeeper().getData(path, watcher, stat);


        while(true) {
            Thread.sleep(5000);
            System.err.println("我还活着呢！！！！！");
        }
    }
}
