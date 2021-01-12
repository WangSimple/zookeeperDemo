package org.simple.zookeeperleader.demo.conf;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.zookeeper.ZookeeperProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
//@EnableConfigurationProperties(ZookeeperProperties.class)
public class ZkConfig {

    private static String scheme = "digest";

    @Value("${spring.cloud.zookeeper.user-passwd}")
    private String userPasswd;

    @Bean(destroyMethod = "close")
    public CuratorFramework curatorFramework(RetryPolicy retryPolicy, ZookeeperProperties properties) throws Exception {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectString(properties.getConnectString());

        if(StringUtils.isNotBlank(userPasswd)){
            builder.authorization(scheme, userPasswd.getBytes("UTF-8"));
        }

        CuratorFramework curator = builder.retryPolicy(retryPolicy).build();
        curator.start();
        log.trace("blocking until connected to zookeeper for " + properties.getBlockUntilConnectedWait() + properties.getBlockUntilConnectedUnit());
        curator.blockUntilConnected(properties.getBlockUntilConnectedWait(), properties.getBlockUntilConnectedUnit());
        log.trace("connected to zookeeper");
        return curator;
    }

    @Bean
    @ConditionalOnMissingBean
    public RetryPolicy exponentialBackoffRetry(ZookeeperProperties properties) {
        return new ExponentialBackoffRetry(properties.getBaseSleepTimeMs(),
                properties.getMaxRetries(),
                properties.getMaxSleepMs());
    }
}
