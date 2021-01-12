package org.simple.zookeeperleader.demo.process;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.locks.LockSupport;

/**
 * @description:
 * @author: xpwang
 * @date: 2020-12-28 2:34 下午
 */
@Slf4j
public class WorkProcess {
    private boolean isStop=true;

    private String task;

    @Setter
    private Thread daemon;

    public void execute(){
        while(!isStop){
            log.warn("{}---{}工作执行中。。。",Thread.currentThread().getName(),task);
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
            }
        }
        log.warn("{}---{}工作停止执行。。。",Thread.currentThread().getName(),task);
        LockSupport.unpark(daemon);
    }

    /**
     * @description: 执行execute()方法前都要先执行Init()
     * @author: xpwang
     * @date: 2021-01-05 14:34
     * @return:
     */
    public void init(Thread daemon){
        this.daemon=daemon;
        isStop=false;
    }

    public boolean isWorking(){
        return !isStop;
    }
    public void stop(){
        isStop=true;
    }

    public WorkProcess(String task){
        this.task=task;
    }

}
