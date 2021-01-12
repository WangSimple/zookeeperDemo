package org.simple.zookeeperleader.demo.listener;

import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

/**
 * @description:
 * @author: xpwang
 * @date: 2020-12-23 2:04 下午
 */
public class LeaderListener implements LeaderLatchListener {


    @Override
    public void isLeader() {

    }

    @Override
    public void notLeader() {

    }

}
