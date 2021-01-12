package org.simple.zookeeperleader.demo.conf;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.simple.zookeeperleader.demo.process.WorkProcess;
import org.simple.zookeeperleader.demo.service.ZookeeperService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.*;

@Slf4j
@Configuration
@AutoConfigureBefore
public class LeaderConfig {


    @Value("${spring.cloud.zookeeper.baseTaskPath: /market/tasks}")
    private String baseTaskPath;
    @Autowired
    private Environment env;
    @Autowired
    private ZookeeperService zkService;
    @Autowired
    private CuratorFramework curatorFramework;

    //所有任务集合
    private Map<String,LeaderSelector> latchMap= Maps.newHashMap();
    //所有工作类
    private Map<String,WorkProcess> processMap= Maps.newHashMap();
    //监听的的task子节点，用于查看task注册了几个服务
    private String listenPath;
    //计算抢主所用时间
    private long contendCost=System.currentTimeMillis();
    //抢到主所用的锁
    private Lock leaderLock = new ReentrantLock();
    //服务Id
    private String applicationName;

    @PostConstruct
    private void init() throws Exception {
        //添加leaderLatch
        List<String> nodeChild = zkService.getNodeChild(baseTaskPath);
        if (nodeChild.size()==0){
            throw new Exception("未获取到zk任务节点。");
        }
        applicationName=env.getProperty("spring.application.name");
        //添加节点监听
        listenPath=baseTaskPath+"/"+nodeChild.get(0);
        this.serviceWatcher();

        nodeChild.stream().forEach(taskpath-> {
            WorkProcess processor=new WorkProcess(taskpath);
            processMap.put(taskpath,processor);
            latchMap.put(taskpath,LeaderSelectorFactory(curatorFramework,baseTaskPath+"/"+taskpath,processor));
        });

    }

    /**
     * @description: 抢到Leader时根据获取到的服务数判断是否需要放弃任务，多线程情况使用方法外面要有锁，要不会造成判断不准
     * @author: xpwang
     * @date: 2020-12-25 17:22
     * @return:
     */
    private boolean abandon(int serverCount){
        return Math.ceil((double)latchMap.size()/serverCount)<=getWorkingCount();
    }

    /**
     * @description: 添加新服务的时候判断是否需要释放掉一些task给新服务
     * @author: xpwang
     * @date: 2021-01-06 20:10
     * @return:
     */
    private boolean release(int serverCount){
        return Math.ceil((double)latchMap.size()/serverCount)<getWorkingCount();
    }



    private int getWorkingCount(){
        //int i= (int) processMap.values().stream().filter(WorkProcess::isWorking).count();
        //log.warn("{}服务运行中的任务有{}个",applicationName,i);//debug一下
        //return i;
        return (int) processMap.values().stream().filter(WorkProcess::isWorking).count();
    }



    private LeaderSelector LeaderSelectorFactory(CuratorFramework curatorFramework,String path,WorkProcess processor){
        LeaderSelector leaderSelector = new LeaderSelector(curatorFramework, path, new LeaderSelectorListenerAdapter() {
            @Override
            public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                boolean isAbandon;
                leaderLock.lock();
                try{
                    List<String> nodeChild = zkService.getNodeChild(path);
                    isAbandon=abandon(nodeChild.size());
                    if (isAbandon){
                        log.warn("{}服务获取到{}任务leader,放弃任务",applicationName,path);
                        return ;
                    }
                }finally {
                    leaderLock.unlock();
                }

                log.warn("{}---{}服务获取到{}任务leader,开始工作。获取任务用时:{} ms",
                        Thread.currentThread().getName(),
                        applicationName,
                        path,
                        System.currentTimeMillis()-contendCost);
                processor.init(Thread.currentThread());
                new Thread(processor::execute).start();
                LockSupport.park();
            }
        });
        leaderSelector.autoRequeue();
        leaderSelector.start();
        return leaderSelector;
    }




    /**
     * @description: 监听task节点下是否增加了服务
     * @author: xpwang
     * @date: 2020-12-24 17:23
     * @return:
     */
    public void serviceWatcher() throws Exception {
        // 为子节点添加watcher
        // PathChildrenCache: 监听数据节点的增删改，可以设置触发的事件
        final PathChildrenCache childrenCache = new PathChildrenCache(curatorFramework, listenPath, true);

        /**
         * StartMode: 初始化方式
         * POST_INITIALIZED_EVENT：异步初始化，初始化之后会触发事件
         * NORMAL：异步初始化
         * BUILD_INITIAL_CACHE：同步初始化
         */
        childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

        // 列出子节点数据列表，需要使用BUILD_INITIAL_CACHE同步初始化模式才能获得，异步是获取不到的
        /*List<ChildData> childDataList = childrenCache.getCurrentData();
        System.out.println("当前节点的子节点详细数据列表：");
        for (ChildData childData : childDataList) {
            System.out.println("\t* 子节点路径：" + new String(childData.getPath()) + "，该节点的数据为：" + new String(childData.getData()));
        }*/
        // 添加事件监听器
        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                // 通过判断event type的方式来实现不同事件的触发
                if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {  // 添加子节点时触发
                    log.debug(Thread.currentThread().getName()+"子节点：" + event.getData().getPath() + " 添加成功");
                    //添加子节点需要判断是否要将任务分配给子节点
                    List<String> nodeChild = zkService.getNodeChild(listenPath);
                    while (release(nodeChild.size())){
                        //找到一个正在执行的任务将其stop
                        Map.Entry<String, WorkProcess> workProcessEntry = processMap.entrySet().stream().filter(entry -> entry.getValue().isWorking()).findFirst().get();
                        workProcessEntry.getValue().stop();
                        log.warn("{}服务释放{}任务",applicationName,workProcessEntry.getKey());
                    }

                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {  // 删除子节点时触发
                    log.debug("子节点：" + event.getData().getPath() + " 删除成功");
                    contendCost=System.currentTimeMillis();
                }
//                else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {  // 修改子节点数据时触发
//                    System.out.print("子节点：" + event.getData().getPath() + " 数据更新成功，");
//                    System.out.println("子节点：" + event.getData().getPath() + " 新的数据为：" + new String(event.getData().getData()));
//                }
//                else if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {  // 子节点初始化时触发
//                    System.out.println("子节点初始化成功");
//                }
            }
        });
    }




    /**
     * @description: 监听是否添加了任务节点
     * @author: xpwang
     * @date: 2020-12-24 17:24
     * @return:
     */
    public void taskWatcher() throws Exception {
        // 为子节点添加watcher
        // PathChildrenCache: 监听数据节点的增删改，可以设置触发的事件
        final PathChildrenCache childrenCache = new PathChildrenCache(curatorFramework, baseTaskPath, true);

        /**
         * StartMode: 初始化方式
         * POST_INITIALIZED_EVENT：异步初始化，初始化之后会触发事件
         * NORMAL：异步初始化
         * BUILD_INITIAL_CACHE：同步初始化
         */
        childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

        // 列出子节点数据列表，需要使用BUILD_INITIAL_CACHE同步初始化模式才能获得，异步是获取不到的
        /*List<ChildData> childDataList = childrenCache.getCurrentData();
        System.out.println("当前节点的子节点详细数据列表：");
        for (ChildData childData : childDataList) {
            System.out.println("\t* 子节点路径：" + new String(childData.getPath()) + "，该节点的数据为：" + new String(childData.getData()));
        }*/
        // 添加事件监听器
        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                // 通过判断event type的方式来实现不同事件的触发
                if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {  // 子节点初始化时触发
                    System.out.println("\n--------------\n");
                    System.out.println("子节点初始化成功");
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {  // 添加子节点时触发
                    System.out.println("\n--------------\n");
                    System.out.print("子节点：" + event.getData().getPath() + " 添加成功，");
                    System.out.println("该子节点的数据为：" + new String(event.getData().getData()));
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {  // 删除子节点时触发
                    System.out.println("\n--------------\n");
                    System.out.println("子节点：" + event.getData().getPath() + " 删除成功");
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {  // 修改子节点数据时触发
                    System.out.println("\n--------------\n");
                    System.out.print("子节点：" + event.getData().getPath() + " 数据更新成功，");
                    System.out.println("子节点：" + event.getData().getPath() + " 新的数据为：" + new String(event.getData().getData()));
                }
            }
        });
    }


    public static void main(String[] args) {
        System.out.println(Math.ceil(0.1)==1);
    }

    //leaderLatch没法重新排队
    /*private LeaderLatch leaderLatchFactory(CuratorFramework curatorFramework,String path ){
        LeaderLatch latch = new LeaderLatch(curatorFramework, path);

        latch.addListener( new LeaderLatchListener() {
            @SneakyThrows
            @Override
            public void isLeader() {
                boolean isAbandon=false;
                workingLock.lock();
                try{
                    List<String> nodeChild = zkService.getNodeChild(path);
                    latch.close();
                    System.out.println(latch.getState());
                    latch.start();
                    System.out.println(latch.getState());
                    isAbandon=abandon(nodeChild.size());
                    if (isAbandon){
                        latch.close();
                        return ;
                    }
                }finally {
                    workingLock.unlock();
                    if (isAbandon){
                        Thread.sleep(1000);
                        latch.start();
                   }

                }
                log.warn("{}服务获取到{}任务leader,开始工作",applicationName,path);
            }

            @Override
            public void notLeader() {
                log.warn("{}服务不是{}任务leader",applicationName,path);
            }
        });
        try{
            latch.start();
        }catch (Exception e){
            log.error("启动zookeeper选举服务异常:", e);
        }
        return latch;
    }*/

    /**
     * @description: 监听当前节点
     * @author: xpwang
     * @date: 2020-12-24 17:22
     * @return:
     */
    /*private void nodeWatcher() throws Exception {

        // NodeCache: 缓存节点，并且可以监听数据节点的变更，会触发事件
        final NodeCache nodeCache = new NodeCache(curatorFramework, baseTaskPath);
        // 参数 buildInitial : 初始化的时候获取node的值并且缓存
        nodeCache.start(true);
        // 为缓存的节点添加watcher，或者说添加监听器
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            // 节点数据change事件的通知方法
            @Override
            public void nodeChanged() throws Exception {
                // 防止节点被删除时发生错误
                if (nodeCache.getCurrentData() == null) {
                    System.out.println("获取节点数据异常，无法获取当前缓存的节点数据，可能该节点已被删除");
                    return;
                }
                // 获取节点最新的数据
                String data = new String(nodeCache.getCurrentData().getData());
                System.out.println(nodeCache.getCurrentData().getPath() + " 节点的数据发生变化，最新的数据为：" + data);
            }
        });
    }*/
}
